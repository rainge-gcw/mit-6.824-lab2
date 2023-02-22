//
// Created by gcw on 23-2-20.
//

#ifndef RAFT2_LOG_H
#define RAFT2_LOG_H
#include "massage.h"
#include <vector>
class Log{
    std::mutex mut;
    std::vector<entries> data;
    std::vector<int>log_term;
public:
    int get_lastLogIndex();
    int get_lastLogTerm();
    int get_preLogIndex(int);
    int get_preLogTerm(int);
    bool check_up_to_date(const std::shared_ptr<json>&);
    bool check_log_contain_pre(const std::shared_ptr<json>&);//检查是否包含 log 不包含 prevLogIndex 中术语与 prevLogTerm 匹配的条目
    bool check_same_idx_but_diff_terms(const std::shared_ptr<json>&);//相同位置但版本不同 如果现有条目与新条目冲突，请删除现有条目
    void append_new_entries(const std::shared_ptr<json>&,int);
    int index_of_last_new_entry();
    void apply_log(const std::string& url,int idx);
    std::vector<entries>get_entries(int match);//把match往后的复制过去
};

bool Log::check_up_to_date(const std::shared_ptr<json>& it) {
    if((*it)["lastLogIndex"].get<int>()>data.size()){
        return true;
    }
    return false;
}
void Log::append_new_entries(const std::shared_ptr<json>&it,int term) {
    std::lock_guard<std::mutex> lk(mut);
    log_term.push_back(term);
    data.push_back({(*it)["method"],(*it)["key"],(*it)["value"]});
}

int Log::get_lastLogIndex() {
    return data.size();
}

int Log::get_lastLogTerm() {
    return log_term[-1];
}

int Log::get_preLogIndex(int idx=-1) {//传过来的是目前要发的
    if(idx==-1)idx=data.size();
    return std::max(0,idx-1);
}

int Log::get_preLogTerm(int idx=-1) {
    if(idx==-1)idx=log_term.size();
    return std::max(0,log_term[idx-1]);
}

bool Log::check_log_contain_pre(const std::shared_ptr<json>& it) {
    int idx=(*it)["prevLogIndex"];
    int term=(*it)["prevLogTerm"];
    if(idx>=data.size())return false;
    if(log_term[idx]!=term)return false;
    return true;
}

bool Log::check_same_idx_but_diff_terms(const std::shared_ptr<json>& it) {
    int idx=(*it)["prevLogIndex"];
    int term=(*it)["prevLogTerm"];
    return log_term[idx]!=term;
}

int Log::index_of_last_new_entry() {
    return data.size()-1;
}

std::vector<entries> Log::get_entries(int match) {
    std::vector<entries>res;
    for(int i=match;i<data.size();i++){
        res.push_back(data[i]);
    }
    return res;
}

#endif //RAFT2_LOG_H
