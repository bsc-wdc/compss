#include <iostream>
#include <fstream>
#include <pthread.h>
#include <sys/types.h>
#include <sys/syscall.h>
//#include <mutex>
#define gettid() syscall(SYS_gettid)


using namespace std;

class customStream : public streambuf {

 private:
   map<int, streambuf*> files;
   streambuf* defaultBuf;
   pthread_mutex_t mtx;
   streamsize xsputn(const char* s, streamsize n){
      if (files.find(gettid()) != files.end()){
          files[gettid()]->sputn(s,n);
      }else {
          defaultBuf->sputn(s,n);
      }
      return n;
   };

   int overflow (int c){
      int ret;
      if (files.find(gettid()) != files.end()){
         ret = files[gettid()]->sputc(c);
      } else {
         ret = defaultBuf->sputc(c);
      }
      return ret;
   };

   // This function is called when a flush is called by endl, adds an endline to the output
   int sync(){
      int ret;
      if (files.find(gettid()) != files.end()){
         ret = files[gettid()]->pubsync();
      } else {
         ret = defaultBuf->pubsync();
      }
      return 0;
   };

 public :

    customStream(streambuf* defsb){
        defaultBuf = defsb;
        pthread_mutex_init(&mtx,NULL);
    };

    //customStream(const char * data, unsigned int len);

    void registerThread(streambuf* threadsb){
       pthread_mutex_lock(&mtx);
       files[gettid()] = threadsb;
       pthread_mutex_unlock(&mtx);
    };

    void unregisterThread(){
       pthread_mutex_lock(&mtx);
       files.erase(gettid());
       pthread_mutex_unlock(&mtx);
    };
};
