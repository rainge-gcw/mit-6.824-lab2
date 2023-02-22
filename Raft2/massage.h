//
// Created by gcw on 23-2-20.
//

#ifndef RAFT2_MASSAGE_H
#define RAFT2_MASSAGE_H
#include "json.hpp"
#include <queue>
#include <utility>
#include "Thread.h"
#include "httplib.h"
using httplib::Request;
using httplib::Response;
using httplib::Server;
using nlohmann::json;
struct entries{
    std::string method;
    std::string key;
    std::string value;
};

struct RequestVote_RPC_Arguments{
    int term;//candidate term
    int candidateId;//candidate requesting vote
    int lastLogIndex;
    int lastLogTerm;
    RequestVote_RPC_Arguments();
    RequestVote_RPC_Arguments(json& js){
        this->term=js["term"];
        this->candidateId=js["candidateId"];
        this->lastLogIndex=js["lastLogIndex"];
        this->lastLogTerm=js["lastLogTerm"];
    }
    void dump(json& js){//将其转换成js
        js["term"]=this->term;
        js["candidateId"]= this->candidateId;
        js["lastLogIndex"]= this->lastLogIndex;
        js["lastLogTerm"]= this->lastLogTerm;
    }
};
struct RequestVote_RPC_result{
    int term;
    int voteGranted;//true mean candidate received vote;

    RequestVote_RPC_result();
    RequestVote_RPC_result(json& js){
        this->term=js["term"];
        this->voteGranted=js["voteGranted"];
    }
    void dump(json& js){//将其转换成js
        js["term"]=this->term;js["voteGranted"]= this->voteGranted;
    }

};

struct AppendEntries_RPC_Arguments{
    int term{};
    int leaderId{};
    int preLogIndex{};
    int preLogTerm{};
    std::vector<entries> data;//空则说明heart 可能会发多个用于efficiency
    int leaderCommit{};//允许写入
    AppendEntries_RPC_Arguments();
    AppendEntries_RPC_Arguments(json& js){//用js来构造
        this->term=js["term"].get<int>();
        this->leaderId=js["leaderId"].get<int>();
        this->preLogIndex=js["preLogIndex"].get<int>();
        this->preLogTerm=js["preLogTerm"].get<int>();
        for(int i=0;true;i++){
            if(js["method"+std::to_string(i)].is_string()){
                entries ent;
                ent.method=js["method"+std::to_string(i)];
                ent.key=js["key"+std::to_string(i)];
                ent.value=js["value"+std::to_string(i)];
                this->data.push_back(ent);
            }else{
                break;
            }
        }
    }
    void dump(json& js){//将其转换成js
        js["term"]=this->term;js["leaderId"]= this->leaderId;
        js["preLogIndex"]=this->preLogIndex,js["preLogTerm"]=this->preLogTerm;
        js["leaderCommit"]= this->leaderCommit;
        for(int i=0;i<this->data.size();i++){
            js["method"+std::to_string(i)]=this->data[i].method;
            js["key"+std::to_string(i)]=this->data[i].key;
            js["value"+std::to_string(i)]=this->data[i].value;
        }
    }
};
struct AppendEntries_RPC_Results{
    int term;
    int success;//true if matching

    AppendEntries_RPC_Results();
    AppendEntries_RPC_Results(json& js){
        this->term=js["term"];
        this->success=js["success"];
    }
    void dump(json& js){//将其转换成js
        js["term"]=this->term;js["success"]= this->success;
    }
};



class Massage{//消息队列 用两个循环来处理发送和接受
public:
    thread_safe_queue<json>get_information;
    thread_pool pool_;
    std::string ip;
    int port;
    //获取一个信息
    void listenLoop();//监听队列
    Massage();
    Massage(std::string ip_,int port_);
    std::shared_ptr<json> get();
    bool send(json &js,const std::string& type,json &res_js,std::string ip_,int port_);//发送一个信息
    int get_term();//返回-1为用户信息 -2为没有
};

Massage::Massage(std::string ip_,int port_) {
    this->ip=std::move(ip_);
    this->port=port_;
    pool_.submit([this]{
        this->listenLoop();
    });
}

void Massage::listenLoop() {
    Server ser;
    ser.Post("/RequestVote_RPC",[&](const Request& req, Response& res){
        json js(req.body);
        js["type"]="RequestVote_RPC";
        this->get_information.push_back(js);
    });
    ser.Post("/AppendEntries_RPC",[&](const Request& req, Response& res){
        json js(req.body);
        js["type"]="AppendEntries_RPC";
        this->get_information.push_back(js);
    });
    ser.listen(this->ip, this->port);
}

bool Massage::send(json &js,const std::string& type,json &res_js,std::string ip_,int port_) {//传入的js需要包含type
    httplib::Client cli(ip_,port_);
    if(auto res=cli.Post(type)){
        if(res->status==200){
            res_js=json(res->body);
            return true;
        }
    }
    return false;
}

std::shared_ptr<json> Massage::get() {
    return get_information.try_pop_front();
}

int Massage::get_term() {
    if(get_information.empty())return -2;//空的
    auto it=get_information.try_pop_front();
    if(!it)return -2;
    if(!(*it)["term"].is_string()){//用户请求
        get_information.push_front(*it);
        return -1;
    }else{
        int term=(*it)["term"].get<int>();
        get_information.push_front(*it);
        return term;
    }
}


#endif //RAFT2_MASSAGE_H
