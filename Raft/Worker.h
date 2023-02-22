//
// Created by gcw on 23-2-17.
//

#ifndef RAFT_WORKER_H
#define RAFT_WORKER_H
#include <vector>
#include <chrono>
#include <string>
#include <queue>
#include "Thread.h"
#include "httplib.h"
#include "json.hpp"
using httplib::Request;
using httplib::Response;
using httplib::Server;
using nlohmann::json;
#define ERROR_CODE "{\"code\":\"1\"}"
#define PASS_CODE "{\"code\":\"0\"}"


void test(){
    auto it=std::chrono::high_resolution_clock::now();
}
struct AppendEntries_RPC{//
    /*
     * RPC的类型
     * 1:AE
     * 2:投票
     */
    std::string type;//AppendEntries_RPC

    /*
     * 意图
     * 1:心跳 heart
     * 2:append 要求记录日志
     * 3:confirm 确认可以写入
     */
    std::string intention;

    /*
     * 任期号
     */
    int term;

    /*
     * 上一个日志的位置
     */
    int pre_log_idx;

    /*
     * 上一个日志的版本
     */
    int pre_log_term;

    /*
     *
     */

};

struct process{
    const Request& req;
    Response& res;
    std::condition_variable& data_cond;
    std::mutex& mut;
};
struct tmp_log{
    int term;
    int idx;
    std::string method;
    std::string key;
    std::string value;
};

class Worker{
    short group_size;//整个系统大小
    int port;//自己的id[0-n]
    std::vector<int>ports;//记录各系统的端口
    short status;//当前状态: 0:follower 1:leader  2:candidate
    std::chrono::duration<unsigned long long,std::ratio<1,1000>>term_size;//发起新一轮选举的时延
    std::chrono::time_point<std::chrono::system_clock> pre_heart_time;//上次心跳时间
    int term;//任期号
    int get_vote;//当前票数
    thread_pool pool_;//线程池
    std::deque<tmp_log>log_queue;
    thread_safe_queue<process>work_queue;//消息队列
    std::mt19937 mt;

    void listen();//监听信息,并存储进消息队列

    void leader_send_massage(tmp_log);




    //需要处理:心跳(leader),请求投票(candidate),追加条目(leader)
    //leader:AppendEntries RPCs 日志复制 心跳
    //candidate:请求投票
    /*
     * AppendEntries RPCs
     *
     *
     */
    void init_heart();
    bool check_heart();
    bool check_pre_log(int pre_log_idx,int pre_log_term);
    bool confirm_write(int pre_log_idx,int pre_log_term);//确认写入
    void send_RequestVote_RPCs();
    int send_AppendEntries_RPC(const Request &req, int aim_port, std::string &res);
    bool check_log_exits(int pre_log_idx,int pre_log_term);
    bool check_newer(int pre_log_idx,int pre_log_term);//检查是否会更新

    void send_heart();

    int voted;

    void start(){
        log_queue.push_back({0,0,"","",""});//防边界问题
        //初始身份为follow

        //设置初始时延
        init_heart();
        while(true){
            if(status==0)//follower
            {
                //检查收件箱 过期版本号无视 最多处理三十个,就要等下一轮了
                    //1:日记信息,重置心跳 更新任期
                    //2:心跳,重置心跳 更新任期
                    //3:投票请求,如果是第一个,(并且自己的日志不比对方的新)后面再加,投出赞同,重置心跳。否者不赞同,不重置心跳 更新任期
                for(int i=1;i<=30&&!work_queue.empty();i++){
                    auto it=work_queue.try_pop();
                    if(!it){
                        break;//空的
                    }
                    json js;
                    std::lock_guard<std::mutex>lk(it->mut);
                    if(it->req.get_param_value("type")=="AppendEntries_RPC"){
                        int leader_term=atoi(it->req.get_param_value("term").c_str());
                        if(leader_term < this->term)//old term
                        {
                            js["term"]=this->term;
                            js["code"]=1;
                        }
                        else//这个else中不需要管term
                        {
                            if(leader_term>this->term){
                                js["term"]=this->term=leader_term;
                                voted=false;
                            }
                            init_heart();
                            if(it->req.get_param_value("intention")=="heart")//心跳
                            {
                                js["code"]=0;
                            }
                            else if(it->req.get_param_value("intention")=="append")//添加
                            {
                                int pre_log_term=atoi(it->req.get_param_value("pre_log_term").c_str());
                                int pre_log_idx=atoi(it->req.get_param_value("pre_log_idx").c_str());
                                js["pre_log_term"]=pre_log_term;
                                js["pre_log_idx"]=pre_log_idx;
                                if(check_pre_log(pre_log_idx,pre_log_term))
                                {
                                    js["code"]=0;
                                }
                                else
                                {
                                    js["code"]=1;
                                }
                            }
                            else if(it->req.get_param_value("intention")=="confirm")//确认可以写入
                            {
                                int pre_log_term=atoi(it->req.get_param_value("pre_log_term").c_str());
                                int pre_log_idx=atoi(it->req.get_param_value("pre_log_idx").c_str());
                                js["code"]=1;
                                if(check_log_exits(pre_log_idx,pre_log_term)&&confirm_write(pre_log_idx,pre_log_term))
                                        js["code"]=0;

                            }
                        }
                    }
                    else if(it->req.get_param_value("type")=="RequestVote_RPCs")
                    {
                        int pre_log_term=atoi(it->req.get_param_value("pre_log_term").c_str());
                        int pre_log_idx=atoi(it->req.get_param_value("pre_log_idx").c_str());
                        int leader_wait_term=atoi(it->req.get_param_value("term").c_str());
                        js["code"]=1;
                        js["term"]=this->term;
                        if(leader_wait_term>this->term&&check_newer(pre_log_idx,pre_log_term)&&!voted)
                        {
                            js["code"]=0;
                            voted=true;
                        }
                    }
                    else//重定向到leader
                    {

                    }

                    it->res.set_content(js.dump(),"text/json");
                    it->data_cond.notify_one();

                }

                //检查心跳如果心跳结束,转换为candidate

                if(!check_heart()){
                    status=2;
                }

            }
            else if(status==1)//leader
            {
                //发送心跳
                send_heart();

                //监听信息 (一次性读取管道) 过期版本号无视
                    //客户端信息,备份发送给follower
                    //follower的确认信息,任期过期:切换follower 如果超过半数,就提交信息给自己的状态机,返回结果给客户端
                    //其他自称leader的日志信息:如果任期过期:切换follower,否者无视
                for(int i=1;i<=30&&!work_queue.empty();i++)
                {
                    auto it=work_queue.try_pop();
                    if(!it){
                        break;//空的
                    }
                    json js;
                    std::lock_guard<std::mutex>lk(it->mut);
                    if(it->req.get_param_value("type")=="AppendEntries_RPC")//另一个自称leader的
                    {
                        int fake_leader_term=atoi(it->req.get_param_value("pre_log_term").c_str());
                        if(fake_leader_term>this->term)//自己过时了
                        {
                            work_queue.push(*it);
                            break;
                        }
                        else//对方是假的
                        {
                            js["code"]=1;
                            js["term"]=this->term;
                        }
                    }
                    else if(it->req.get_param_value("type")=="client")//平常用户
                    {
                        tmp_log tmpLog{};
                        tmpLog.term = this->term;
                        tmpLog.idx = log_queue.size();
                        tmpLog.method=it->req.get_param_value("method");
                        tmpLog.key=it->req.get_param_value("key");
                        tmpLog.value=it->req.get_param_value("value");
                        log_queue.push_back(tmpLog);


                        leader_send_massage(tmpLog);//记录得票,数量足够就直接在函数内写入 票数足够就写入 然后通知子群写入 循环的时候要注意自己的状态




                    }
                    else if(it->req.get_param_value("type")=="RequestVote_RPCs")//请求投票,检查对方版本
                    {
                        int fake_vote_term=atoi(it->req.get_param_value("pre_log_term").c_str());
                        if(fake_vote_term > this->term)//对方版本高 切换状态
                        {
                            work_queue.push(*it);
                            status=0;
                            break;
                        }
                        else//对方是假的
                        {
                            js["code"]=1;
                            js["term"]=this->term;
                        }
                    }
                    it->res.set_content(js.dump(),"text/json");
                    it->data_cond.notify_one();
                }//end for

            }
            else//candidate
            {
                    //自增当前任期号 给自己投票,发出投票请求
                this->term++;
                voted=true;
                this->get_vote=1;
                send_RequestVote_RPCs();//子进程中需要添加票
                auto start_time=std::chrono::system_clock::now();
                bool exit_code=false;//角色出现转换
                while(true)
                {
                    auto end_time=std::chrono::system_clock::now();
                    auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time);
                    if(duration.count()>100)//重新开始新一轮
                        break;

                    if(get_vote>=this->group_size/2+1)
                    {
                        status=1;
                        exit_code=true;
                        voted=false;
                        break;
                    }

                    for(int i=1;i<=10&&!work_queue.empty();i++)
                    {
                        auto it=work_queue.try_pop();
                        if(!it){
                            break;//空的
                        }
                        json js;
                        if(it->req.get_param_value("type")=="AppendEntries_RPC")//只有可能是leader发来的
                        {
                            int leader_term=atoi(it->req.get_param_value("term").c_str());
                            if(leader_term<this->term)//过时
                            {
                                js["code"]=1;
                                js["term"]=this->term;
                                it->res.set_content(js.dump(),"text/json");
                                it->data_cond.notify_one();
                            }
                            else if(leader_term>=this->term)//转换成follow
                            {
                                this->term=leader_term;
                                init_heart();
                                work_queue.push(*it);
                                this->status=0;
                                exit_code=true;
                            }
                        }
                        else if(it->req.get_param_value("type")=="RequestVote_RPCs")//其他人的拉票请求
                        {
                            int voter_term=atoi(it->req.get_param_value("term").c_str());
                            if(voter_term>this->term)//自己过时
                            {
                                init_heart();
                                work_queue.push(*it);
                                this->status=0;
                                exit_code=true;
                                voted=false;
                            }
                            else//对方平等或对方过时
                            {
                                js["code"]=1;
                                js["term"]=this->term;
                                it->res.set_content(js.dump(),"text/json");
                                it->data_cond.notify_one();
                            }
                        }
                        if(exit_code){
                            break;
                        }
                    }

                    if(exit_code)
                        break;
                }
            }//end_candidate
        }//end_while
    }//end_start

    int rand_int(int l, int r);


    void send_confirm(tmp_log tmpLog);
};

void Worker::listen() {
    Server ser;

    ser.Post ("/massage", [&](const Request& req, Response& res){
        std::condition_variable data_cond;
        std::mutex mut;
        work_queue.push({req,res,data_cond,mut});//复制,之后就直接舍弃了
        std::unique_lock<std::mutex> lk(mut);
        data_cond.wait(lk,[&]{//条件一开始不满足,解锁mut,阻塞
            return !res.body.empty();
        });
    });


    ser.listen("0.0.0.0",this->port);
}
int Worker::rand_int(int l,int r){
    return mt()%(r-l+1)+l;
}

void Worker::init_heart() {
    this->pre_heart_time=std::chrono::high_resolution_clock::now();
    this->term_size=std::chrono::duration<unsigned long long,std::ratio<1,1000>>(rand_int(150,300));
}

bool Worker::check_heart() {
    return std::chrono::system_clock::now() - this->pre_heart_time > this->term_size;
}

int Worker::send_AppendEntries_RPC(const Request &req,int aim_port,std::string& res) {
    httplib::Client cli("172.0.0.1",aim_port);
    auto it=cli.Post("/massage",req.headers,req.body,"text/json");
    if(it->status==200){
        return 1;
    }else{
        res=it->body;
        return 0;
    }
}

void Worker::send_heart()
{
    json js;
    Request req;
    js["type"]="AppendEntries_RPC";
    js["term"]=this->term;
    js["intention"]="heart";
    req.body=js.dump();
    for(const auto &i:ports){
        if(this->status!=1)break;
        if(i==this->port)continue;
        std::string tmp;
        send_AppendEntries_RPC(req,i,{tmp});
        json js2(std::move(tmp));
        tmp=js2["term"];
        if(atoi(tmp.c_str())>this->term)
        {
            this->status=0;//变为follower
        }
    }
}

void Worker::leader_send_massage(tmp_log tmpLog) {
    //一直发
    int received_cnt;
    //当前的,和上一个
    int pre_term=this->log_queue[this->log_queue.size()-2].term;
    int pre_idx= this->log_queue.size()-1;
    for(const auto i:ports){
        pool_.submit([i,&received_cnt,tmpLog,  pre_term,pre_idx,this]{
            json js;
            js["term"]=tmpLog.term;
            js["idx"]= tmpLog.idx;
            js["method"]=tmpLog.method;
            js["key"]=tmpLog.key;
            js["value"]=tmpLog.value;
            js["pre_term"]=pre_term;
            js["pre_idx"]=pre_idx;
            std::string res;
            Request req;
            req.body=js.dump();
            while(send_AppendEntries_RPC(req,i,res)){
                if(this->status!=1)//即使结束
                    return;
            }
            js=json(res);
            if(js["code"]==0)
            {
                received_cnt++;
            }
            else//处理
            {
                std::string tmp=js["term"];
                if(atoi(tmp.c_str())>this->term)//版本落后导致的失败
                {
                    this->status=0;return;
                }
                else//日志不匹配导致的失败
                {

                }
            }
        });
    }
    while(received_cnt <= this->group_size/2)
    {
        if(this->status!=1)return;//时刻检查身份
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }



}
void Worker::send_confirm(tmp_log tmpLog)
{
    for(const auto i:ports){
        pool_.submit([i,tmpLog, this]{
            json js;
            js["term"]=tmpLog.term;
            js["idx"]= tmpLog.idx;
            js["method"]=tmpLog.method;
            js["key"]=tmpLog.key;
            js["value"]=tmpLog.value;
            std::string res;
            Request req;
            req.body=js.dump();
            while(send_AppendEntries_RPC(req,i,res)){
                if(this->status!=1)//即使结束
                    return;
            }

        });
    }
}



#endif //RAFT_WORKER_H
