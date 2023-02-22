//
// Created by gcw on 23-2-20.
//

#ifndef RAFT2_RAFT_H
#define RAFT2_RAFT_H

#include <utility>

#include "Thread.h"
#include "massage.h"
#include "log.h"

struct time_killer{
    bool killed();
};

class Raft{
    int role{};
    int id;
    int group_size;
    std::string log_url;
    std::vector<int>ports;
    std::vector<std::string>ips;
    //常驻状态
    int currentTerm{};
    int votedFor{};
    Log log;
    //易失状态
    int commitIndex{};//已知已提交的最高日志条目的索引
    int lastApplied{};//应用于状态机的最高日志条目的索引
    //计时器
    std::mt19937 mt;
    std::chrono::duration<unsigned long long,std::ratio<1,1000>>term_size;//发起新一轮选举的时延
    std::chrono::time_point<std::chrono::system_clock> pre_heart_time;//上次心跳时间
    //leader状态
    std::vector<int>nextIndex;//要发送到该服务器的下一个日志条目的索引 记录各个follower上下一条应该发送日志的坐标
    std::vector<int>matchIndex;//已知follower上，从0开始有多少条连续的log entry与leader一致。
    thread_pool pool_;//处理多线程
    Massage massage;

    void becomeFollower();
    void becomeCandidate();

    void becomeLeader();
    void electionLoop();//由time驱动选举,后台常驻,为leader时跳出(candidate也跳)
    void pingLoop();//后台常驻,不是leader时continue
    void applyLoop();//将已经 commit 的 log 不断应用到状态机 后台常驻 goroutine
    //角色
    void Follower();
    void Leader();
    void Candidate();
    //处理各类消息
    void process_RequestVote_RPC(std::shared_ptr<json>);
    void process_AppendEntries_RPC(std::shared_ptr<json>);


    //主服务器
    void server();

    //其他
    void reset_election_timer();
    void send_RequestVote_to_all_other(int&);
    void send_AppendEntries_RPC_to_all_other();
public:
    Raft(int id_,int group_size_,std::string log_url_,std::vector<int>ports_,std::vector<std::string>ips_):
                id(id_),group_size(group_size_),log_url(std::move(log_url_)),ports(ports_),ips(ips_), massage(),
                log()
                {
        this->nextIndex.resize(group_size_);
        this->matchIndex.resize(group_size_);
        this->massage.ip=ips_[id_];
        this->massage.port=ports_[id_];
        mt=std::mt19937(std::chrono::system_clock::now().time_since_epoch().count());
    }

    int rand_int(int l, int r);
};

void Raft::server() {
    pool_.submit([this]{
        this->electionLoop();
        this->applyLoop();
        this->pingLoop();
    });
    reset_election_timer();
    while(true){
        switch (this->role) {
            case 0:
                Follower();
                break;
            case 1:
                Leader();
                break;
            case 2:
                Candidate();
                break;
        }
    }
}


void Raft::Follower() {
    while(this->role==0){
        int term=massage.get_term();
        if(term==-2)
            continue;
        if(term > this->currentTerm){
            this->currentTerm=term;
            break;
        }
        auto it=massage.get();//非阻塞的get
        if(!it)
            continue;

        if((*it)["type"]=="RequestVote_RPC"){
            process_RequestVote_RPC(it);
        }else if((*it)["type"]=="AppendEntries_RPC"){
            process_AppendEntries_RPC(it);
        }
    }
}

void Raft::Candidate() {
    while(this->role==2){//进行一轮选举
        ++this->currentTerm;
        this->votedFor=this->id;
        int get_vote=1;
        reset_election_timer();
        send_RequestVote_to_all_other(get_vote);
        time_killer timeKiller;
        while(this->role==2&&!timeKiller.killed()){//接受信息,定时断开

            if(get_vote>=this->group_size/2+1){
                becomeLeader();
            }

            int term=massage.get_term();
            if(term==-2)
                continue;
            if(term > this->currentTerm){
                becomeFollower();
                this->currentTerm=term;
                break;
            }

            auto it=massage.get();//非阻塞的get
            if(!it)
                continue;

            if((*it)["type"]=="RequestVote_RPC"){
                process_RequestVote_RPC(it);
            }else if((*it)["type"]=="AppendEntries_RPC"){
                process_AppendEntries_RPC(it);
            }
        }
    }
}

void Raft::Leader() {
    while(this->role==1){
        int term=massage.get_term();
        if(term==-2)
            continue;
        if(term==-1){//用户信息
            auto it=massage.get();
            if(!it)
                continue;
            //append_to_log
            log.append_new_entries(it,this->currentTerm);

            //用for循环遍历 leader不能给自己发 满足一半再回去,记得commit++
            send_AppendEntries_RPC_to_all_other();

            //update commitIndex

            //respond user


        }else{//其他服务器
            if(term > this->currentTerm){
                becomeFollower();
                this->currentTerm=term;
                break;
            }



        }

    }
}

void Raft::process_RequestVote_RPC(std::shared_ptr<json> it) {
    int term=(*it)["term"].get<int>();
    int candidateId=(*it)["candidateId"].get<int>();

    if(term < this->currentTerm){
        //refuse(5.1)

    }else if(this->votedFor==-1||this->votedFor==candidateId){
        if(log.check_up_to_date(it)){
            //vote(5.2)
        }else{
            //refuse(5.4)
        }
    }
}

void Raft::process_AppendEntries_RPC(std::shared_ptr<json> it) {
    int term=(*it)["term"].get<int>();
    int leaderCommit=(*it)["leaderCommit"].get<int>();
    if(term < this->currentTerm){
        //refuse(5.1)
        return;
    }
    if(!log.check_log_contain_pre(it)){
        //refuse(5.3)
        return;
    }
    if(log.check_same_idx_but_diff_terms(it)){
        //delete and follow
    }else{
        //append
        log.append_new_entries(it,(*it)["term"].get<int>());
    }

    if(leaderCommit>commitIndex){
        commitIndex=std::min(leaderCommit,log.index_of_last_new_entry());
    }

    //allow
}


void Raft::applyLoop() {
    while(true){
        while(this->commitIndex > this->lastApplied){
            ++this->lastApplied;
            log.apply_log(this->log_url,this->lastApplied);
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(30));
    }
}

void Raft::pingLoop() {
    while(true){
        if(this->role==1){
            json js;
            json tmp;
            js["term"]=this->currentTerm;
            js["leaderId"]=this->id;
            js["preLogIndex"]= this->log.get_preLogIndex();
            js["preLogTerm"]= this->log.get_preLogTerm();
            js["leaderCommit"]=this->commitIndex;
            for(int i=0;i<this->group_size;i++){
                if(i==this->id)continue;
                massage.send(js,"RequestVote_RPC",tmp,ips[i],ports[i]);
            }
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(30));
    }
}

void Raft::becomeFollower() {
    this->role=0;
}

void Raft::becomeLeader() {
    this->role=1;
}

void Raft::becomeCandidate() {
    this->role=2;
}
int Raft::rand_int(int l,int r){
    return mt()%(r-l+1)+l;
}
void Raft::electionLoop() {
    while(true){
        if(this->role==0){
            if(std::chrono::system_clock::now() - this->pre_heart_time > this->term_size){
                becomeCandidate();
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(30));
        }
    }
}

void Raft::reset_election_timer() {
    this->pre_heart_time=std::chrono::high_resolution_clock::now();
    this->term_size=std::chrono::duration<unsigned long long,std::ratio<1,1000>>(rand_int(150,300));
}

void Raft::send_RequestVote_to_all_other(int& vote) {
    json js;
    json res_js;
    js["term"]=this->currentTerm;
    js["candidateId"]=this->id;
    js["lastLogIndex"]=this->log.get_lastLogIndex();
    js["lastLogTerm"]=this->log.get_lastLogTerm();
    for(int i=0;i<this->group_size;i++){
        if(i==this->id)continue;
        if(massage.send(js,"RequestVote_RPC",res_js,ips[i],ports[i])){
            if(res_js["success"]=="1")
                ++vote;
        }
    }
}

void Raft::send_AppendEntries_RPC_to_all_other() {
    int commit_cnt=1;
    for(int i=0;i<this->group_size;i++){
        if(i==this->id)continue;
        AppendEntries_RPC_Arguments AE;
        AE.data=log.get_entries(nextIndex[i]);
        AE.term=this->currentTerm;
        AE.leaderCommit=this->commitIndex;
        AE.preLogIndex=log.get_preLogIndex(nextIndex[i]);
        AE.preLogTerm=log.get_preLogTerm(nextIndex[i]);
        AE.leaderId=this->id;
        json js;
        AE.dump(js);
        int data_size=AE.data.size();
        pool_.submit([this,&js,i,&commit_cnt,data_size]{
            json tmp;
            while(!massage.send(js,"AppendEntries_RPC",tmp, this->ips[i], this->ports[i])){
                if(tmp["success"]=="1"){
                    ++commit_cnt;
                    this->matchIndex[i]+=data_size;
                    this->nextIndex[i]=this->matchIndex[i]+1;
                }
                else if(tmp["term"].get<int>() > this->currentTerm){
                    becomeFollower();
                    break;
                }else if(tmp["success"]=="0"){//失败,下次再来
                    --this->matchIndex[i];
                    this->nextIndex[i]=this->matchIndex[i]+1;
                }
            }
        });
    }
    while(this->role==1&&commit_cnt<this->group_size/2+1){
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
}


#endif //RAFT2_RAFT_H
