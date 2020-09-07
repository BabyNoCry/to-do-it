/*
*不同的浏览器对url的长度的限制是不同的，通过查询了解，大多数url的上限为2048B
*1GB = 1024 * 1024 *1024 B
*1GB 内存最多可以存储 512 * 1024 = 524288 个url
* 阻塞队列中数据数量限制就为524288 
* 设定url文件名为 url.txt
* 最终输出的top100 url文件为 result.txt
*/


#include<iostream>
#include<queue>
#include<pthread.h>
#include<stdio.h>
#include<unistd.h>
#include<fstream>
#define MAX_NUM 524288 
#define MAX_THR 5  //线程数

Ofstream out;
pthread_mutex_t read_mutex;
pthread_mutex_t write_mutex;
bool flag = false; //标志文件是否读取完毕
vector<ofstream> v(100); // 100个小文件的写操作流
vector<ifstream >read(100); //100个小文件的读操作流
unordered_map<string,int> mp;

class BlockQueue
{
private:
    std::queue<string> _queue;
    int _capacity;
    pthread_cond_t _cond_pro;
    pthread_cond_t _cond_con;
    pthread_mutex_t mutex;
public:
    BlockQueue(int que_Maxcapacity)
      :_capacity(que_Maxcapacity)
    {
      pthread_mutex_init(&mutex,NULL);
      pthread_cond_init(&_cond_pro,NULL);
      pthread_cond_init(&_cond_con,NULL);
    }

    ~BlockQueue()
    {
      pthread_mutex_destroy(&mutex);
      pthread_cond_destroy(&_cond_pro);
      pthread_cond_destroy(&_cond_con);
    }
    
    //提供给生产者的接口（数据入队）
    bool queuePush(string& data)
    {
      //queue是一个临界资源所以需要加锁保护
      pthread_mutex_lock(&mutex);
      //判断队列是否添加满了
      while(_queue.size() == _capacity)
{
        pthread_cond_wait(&_cond_pro,&mutex);
      }
      _queue.push(data);
      pthread_mutex_unlock(&mutex);
      pthread_cond_signal(&_cond_con);
      return true;
    }
    //提供给消费者的接口（数据出队）
    bool queuePop(string& data)
    {
      pthread_mutex_lock(&mutex);
      //判断队列是否为空
      while(_queue.empty()){

        pthread_cond_wait(&_cond_con,&mutex);
      }
      data = _queue.front();
      _queue.pop();
      pthread_mutex_unlock(&mutex);
      pthread_cond_signal(&_cond_pro);
      return true;
    }
};

void* pro_thr(void* arg)
{
  int i = 0;
  BlockQueue* queue = (BlockQueue*)arg;
  while(1){
pthread_mutex_lock(&read_mutex);
String str;
If(getline(out,str))
{
pthread_mutex_lock(&read_mutex);
flag = true;
break;

}
pthread_mutex_lock(&read_mutex);

    queue->queuePush(i);
  }
  return NULL;
}

void* con_thr(void* arg)
{

string data;

  BlockQueue* queue = (BlockQueue*)arg;
  while(1){
If(queue.empty()&& flag == true){
Break;
}
queue->queuePop(data);

long num = Change(data);
pthread_mutex_lock(&write_mutex);

v[num]<<data<<endl;  //将url写入小文件
pthread_mutex_unlock(&write_mutex);


  }
  return NULL;
}


//将url转化为数字并求其哈希映射,同时写入文件
long change(string url){
long num = 0;
for(int i = 0; i < url.size(); i++){
Num += url[i];
}
return num % 100;
}

//读取url文件，并且将url写入对应的小文件
void cut_write(){
out.open(“url.txt”,ios:in); //以读的方式打开大文件
string  url = “url”;
//以追加的方式打开100个小文件
for(int i = 0; i < 100; i++){
String tmp = url + to_string(i) + “.txt”;
V[i].open(tmp.c_str(),ios::app);

}
  BlockQueue queue(MAX_NUM);
  pthread_t pro_tid[MAX_THR];
  pthread_t con_tid[MAX_THR];
  int i = 0;
  int ret = 0;

//五个读取线程，五个写入线程
  for(i = 0 ; i < MAX_THR; i++)
  {
    pthread_create(&pro_tid[i],NULL,pro_thr,(void*)&queue);
    if(ret != 0)
    {
      std::cerr << "pthread_create pro_thr error\n";
      return -1;
    }
  }

  for(i = 0; i < MAX_THR; i++)
  {
    pthread_create(&con_tid[i],NULL,con_thr,(void*)&queue);
    if(ret != 0)
    {
      std::cerr << "pthread_create pro_thr error\n";
      return -1;
    }
  }

  for(i = 0; i < MAX_THR; i++)
  {
    pthread_join(pro_tid[i],NULL);
    pthread_join(con_tid[i],NULL);
  }
for(int i = 0; i < 100; i++){
String tmp = url + to_string(i) + “.txt”;
v[i].close();

}
}

//统计每个url出现的次数
Void count_size(){
//以读的方式打开文件，
String str = “url”;
for(int i = 0 ; i<100; i++){
String tmp = str + to_string(i) + “.txt”;
read[i].open(“url.txt”,ios:in);)
String data  ;
int count = 0;
While(getline(read[i],data)){
If(mp.find(data) != mp.end() )
mp[data]++; 
else{
mp[data] = 1;
}
}
read[i].close();
}
}

struct cmp{
bool operator(pair<string,int>p1,pair<string,int>p1){
return p1.second > p2.secnd;
}
}

//获得top100的url
void get_top(){
if(mp.size() >= 100){
//url的种数大于100，则构建元素个数为100的小堆
priority_queue<pair<string,int>,vector<pair<string,int>>,cmp> 	pq(mp.begin(),mp.begin() + 100);
auto it = mp.begin() + 100;
While(it != mp.end()){
If(it->second > pq.top().second){
pq.pop();
pq.push(*it);
}
}

//目前堆中的元素为top100，将其保存到文件中
ofstream  file(“result.txt”,ios::out);

While(!pq.empty()){
file << pq.top().first<<endl;
pq.pop();
}
File.close();

}
else{
//	不存在top100，url种数小于 100
}

}
int main()
{
cut_write();   // 分割文件
 count_size();  //统计所有小文件中 url 出现的次数
get_top();     //获取top100的url保存至result文件中
  return 0;
}

