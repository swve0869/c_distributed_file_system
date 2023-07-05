/*
Distributed File System Server
Author: Swami Velamala
*/


#include <stdio.h>
#include <string.h>	//strlen
#include <sys/socket.h>
#include <sys/sendfile.h>
#include <arpa/inet.h>	//inet_addr
#include <unistd.h>	//write
#include <stdlib.h>
#include <netdb.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/file.h>
#include <sys/time.h>
#include <fcntl.h>
#include <time.h>
#include <errno.h>
#include <dirent.h>
#include <errno.h>

#define BUFSIZE 1024

off_t fsize(const char* filename){
    struct stat st;
    if(stat(filename,&st)== 0)
        return st.st_size;

    //fprintf(errno,"stat failed\n");
    
    return -1;
}


int main(int argc , char *argv[])
{   
	if(argc < 3){
		printf("not enough args\n");
		return -1;
	}

	int port = atoi(argv[2]);
	if(port <= 1024){
		printf("invalid port #\n");
		return -1;
	}

    char* server_dir = argv[1];
    strcat(server_dir,"/");

    printf("dir: %s  port: %d\n",server_dir,port);

	int socket_desc , client_sock , c , read_size;
	struct sockaddr_in server , client;

    //Create socket
	socket_desc = socket(AF_INET , SOCK_STREAM , 0);
	if (socket_desc == -1)
	{
		printf("Could not create socket");
	}


	
	//Prepare the sockaddr_in structure
	server.sin_family = AF_INET;
	server.sin_addr.s_addr = INADDR_ANY;
	server.sin_port = htons(port);
    int a = 1;
	setsockopt(socket_desc, SOL_SOCKET, SO_REUSEADDR, &a, sizeof(int));


	//Bind
	if( bind(socket_desc,(struct sockaddr *)&server , sizeof(server)) < 0)
	{
		//print the error message
		perror("bind failed. Error");
		return 1;
	}
	
	//Listen
	listen(socket_desc , 5);
	
    while(1){
        //Accept and incoming connection
        puts("Waiting for incoming connections...");
        c = sizeof(struct sockaddr_in);

        //accept connection from an incoming client
        client_sock = accept(socket_desc, (struct sockaddr *)&client, (socklen_t*)&c);


        if (client_sock < 0)
        {
            perror("accept failed");
            return 1;
        }
        puts("Connection accepted");

        if(fork() == 0)
            break;

        close(client_sock);
    }

   /* struct timeval timeout2;
    timeout.tv_sec = 1;
    timeout.tv_usec = 0;
    if (setsockopt(client_sock, SOL_SOCKET, SO_RCVTIMEO, (char *)&timeout2, sizeof(timeout2)) < 0) {
        perror("setsockopt failed");
        exit(EXIT_FAILURE);
    }*/
    

    char buf[BUFSIZE];
    recv(client_sock,buf,BUFSIZE,0);

        if(strstr(buf,"put")){
            char * filename;
            size_t fsize;
            strtok(buf," ");
            filename = strtok(NULL," ");
            fsize = (size_t)atoi(strtok(NULL," "));

            printf("put %s  %lu\n",filename,fsize);

            send(client_sock,"ok",3,0);
    
            char path[200]; bzero(path,200); strcat(path,server_dir); strcat(path,filename);
            int fd = open(path,O_WRONLY|O_CREAT, 0666);
            bzero(buf,BUFSIZE);
            while(fsize > 0){
                int bytes_received = recv(client_sock,buf,BUFSIZE,0);
                write(fd,buf,bytes_received);
                fsize = fsize - bytes_received;
                bzero(buf,BUFSIZE);
            }
            close(fd);
        }

        if(strstr(buf,"ls")){
            DIR* dir;
            struct dirent *entry;
            char availables_files[3000]; bzero(availables_files,3000);

            dir = opendir(server_dir);
            if (dir == NULL) {
                perror("opendir failed");
                return 1;
            }

            while ((entry = readdir(dir)) != NULL) {
                // Ignore "." and ".." files
                if (strcmp(entry->d_name, ".") != 0 && strcmp(entry->d_name, "..") != 0) {
                    strcat(availables_files,entry->d_name);
                    strcat(availables_files," ");
                }
            }

            int bytes_sent = send(client_sock,availables_files,3000,0);
            
        }

        if(strstr(buf,"get")){
            char * filename;
            strtok(buf," ");
            filename = strtok(NULL," ");

            char path[200]; strcat(path,server_dir); strcat(path,filename); 
   
            // implement file locking !!!!!
            off_t filesize = fsize(path+3);

            bzero(buf,BUFSIZE);
            sprintf(buf,"%ld.",filesize);
            send(client_sock,buf,strlen(buf),0);
            bzero(buf,BUFSIZE);
            recv(client_sock,buf,3,0);
            if(strcmp("ok",buf) != 0){
                printf("no ack received");
                return -1;
            }


            int fd = open(path+3,O_RDONLY);
            if(fd == -1){
                printf("fopen failed %s\n",strerror(errno));
            }

            int bytes_sent = sendfile(client_sock,fd,0,filesize);
            close(fd);

        }

    close(client_sock);
    exit(0); 
}