```c++
#include "Raft.h"
#include "Thread.h"
std::vector<std::string>ips({".","127.0.0.1","127.0.0.1","127.0.0.1"});
std::vector<int>ports({0,10020,10021,10022});
//cpp group_size id log_url ip1 ip2 ip3 ip4 port1 port2 port3 port4
int main(int argc, char * const argv[]) {
    Raft raft;
    raft.group_size= atoi(argv[0]);
    raft.id=atoi(argv[1]);
    raft.log_url=argv[2];
    freopen(argv[2], "w", stdout);
    for(int i=0;i<raft.group_size;i++){
        raft.ips.emplace_back(argv[i+2]);
    }
    for(int i=raft.group_size+2;i<2*raft.group_size+2;i++){
        raft.ports.push_back(atoi(argv[i]));
    }
    raft.work();
}
```
