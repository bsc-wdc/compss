#include <jni.h>
#include <streambuf>
#include <cstdlib>
#include <cstdio>
#include <algorithm>
#include <cstring>
#include <iostream>

using std::size_t;

#include "JavaNioConnStreamBuffer.h"

  //Ctor takes env pointer for the working thread and java.io.PrintStream
  JavaNioConnStreamBuffer::JavaNioConnStreamBuffer(JNIEnv* jni_env, jobject  niostream, unsigned int buffsize)
  {
     this->jni_env = jni_env;
     this->handle = jni_env->NewGlobalRef(niostream);
     this->size = buffsize;
     this->put_back_= size_t(1);
     this->buff= new char[buffsize];
     this->next_w_element=0;
     this->j_value = NULL;
     char *end = &buff[0] + size;
     setg(end, end, end);
  }

  //This method  the central output of the streambuf class, every charakter goes here
  std::streambuf::int_type JavaNioConnStreamBuffer::overflow(std::streambuf::int_type in = traits_type::eof()){
	  //printf(" [JSB-OF] Calling overflow %c \n", in);
	  buff[next_w_element]=in;
	  next_w_element++;
	  if(in == std::ios::traits_type::eof() ||  next_w_element == size){
         jobject o = jni_env->NewDirectByteBuffer(buff,sizeof(char)*next_w_element);
         //jbyteArray o = env->NewByteArray(next_w_elementsize);
         //env->SetByteArrayRegion(o,0,,(jbyte*)&buff[0]);
         printf(" [JSB-OF] Calling push \n");
         fflush(NULL);
         jmethodID id = jni_env->GetMethodID(jni_env->GetObjectClass(handle),"push","(Ljava/nio/ByteBuffer;)V");
         if (o !=NULL && id!=NULL){
        	jni_env->CallVoidMethod(handle,id,o);
        	if (jni_env->ExceptionOccurred()) {
        		  printf(" [JSB.OF] Exception calling method \n");
        		  jni_env->ExceptionDescribe();
        		  exit(0);
            }
        	 /*if(in == EOF && )
        		 jni_env->CallVoidMethod(handle,id,NULL);
        	 else{*/
        	buff[0]=in;
        	next_w_element=0;
        	 //}
        	 //printf(" [JSB]  finished. \n");
        	 //this->std::streambuf::overflow(in);
        	 //printf(" [JSB.OF] push finished. \n");
        	 //fflush(NULL);
         } else {
        	 printf("Error: PUSH Method ID is null");
        	 exit(0);
         }

     }
     return in;
  }
  std::streambuf::int_type JavaNioConnStreamBuffer::underflow(){
	  if (gptr() < egptr()){ // buffer not exhausted
		  std::streambuf::int_type out = traits_type::to_int_type(*gptr());
		  //printf(" [JSB-UF] Returning %d\n", out);
		  //fflush(NULL);
		  return out;
	  }
	  // Buffer exhausted
      //char *base = &buff[0];
      //char *start = base;

      /*if (eback() == base) // true when this isn't the first fill
      {
          // Make arrangements for putback characters
    	  printf(" [JSB-UF] Doing put_back characters \n");
    	  fflush(NULL);
          std::memmove(base, egptr() - put_back_, put_back_);
          start += put_back_;
      }*/

      // start is now the start of the buffer, proper.
      // Read from fptr_ in to the provided buffer
      printf(" [JSB-UF] Calling pull \n");
      fflush(NULL);
      if (j_value != NULL){
    	  printf(" [JSB-UF] Releasing previous bytearray \n");
    	  jni_env->ReleaseByteArrayElements(j_value, (jbyte*)buff, 0);
      }
	  jmethodID id = jni_env->GetMethodID(jni_env->GetObjectClass(handle),"pull","()[B");
	  if (id != NULL){
		  //printf(" [JSB-UF] Calling pull \n");
		  //fflush(NULL);
		  j_value = (jbyteArray)jni_env->CallObjectMethod(handle,id);
		  //j = jni_env->CallIntMethod(handle,id,o);
		  if (jni_env->ExceptionOccurred()) {
			  printf(" [JSB-UF] Exception calling method \n");
			  jni_env->ExceptionDescribe();
			  exit(0);
		  }
		  if(j_value != NULL)
		  {
		          size = jni_env->GetArrayLength(j_value);
		          if (size == 0){
		        	  printf(" [JSB-UF] Underflow returning EOF \n");
		        	  fflush(NULL);
		        	  return traits_type::eof();
		          }
		          buff = (char*) jni_env->GetByteArrayElements(j_value, NULL);
		          // Set buffer pointers
		          char *base = buff;
		          char *start = base;
		          char *end = base + size;
		          printf(" [JSB-UF] Setg finished with values %d %d %d \n", base, start, end);
		          fflush(NULL);
		          setg(base, start, end);
		          std::streambuf::int_type out = std::ios::traits_type::to_int_type(buff[0]);
		          printf(" [JSB-UF] Returning (pointer %d) %d\n", buff, out);
		          fflush(NULL);
		          return out;
		  }else{
			  printf(" [JSB-UF] Underflow returning null, sending EOF \n");
			  fflush(NULL);
			  return traits_type::eof();
		  }

	  } else {
	  	  printf("Error: PULL Method ID is null");
	  	  fflush(NULL);
	  	  exit(0);
	  }
  }

  JavaNioConnStreamBuffer::~JavaNioConnStreamBuffer()
  {
     overflow();
     if (j_value != NULL){
      	  //printf(" [JSB-UF] Releasing previous bytearray \n");
       	  jni_env->ReleaseByteArrayElements(j_value, (jbyte*)buff, 0);
     }
     jni_env->DeleteGlobalRef(handle);
  }
