/*
	Swami Velamala 
	C proxy server
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

#define SERVERCOUNT 4
#define BUFSIZE 1024

struct schema{
    int serv_num;
    int chunks; 
};

// struct to keep track of available files and chunks
struct srv_file_struct{
    char filename[100];
    char time_stamp[30];
    int chunkcount[SERVERCOUNT];
    char chunk_tracker[SERVERCOUNT][20];
    struct srv_file_struct * next_file;
    long double dbl_time_stamp;
    int ignore;
};

// get a files size in bytes
off_t fsize(const char* filename){
    struct stat st;
    if(stat(filename,&st)== 0)
        return st.st_size;
    return -1;
}

int parse_fname_from_path(char * path, char* filename){
    int i;
    for(i = strlen(path); i > 0; i --){
        if(path[i] == '/')
            break;
    }

    if(i == 0){
        memcpy(filename,path,strlen(path)-i);
        return 0;
    }
    memcpy(filename,path+i+1,strlen(path)-i); 

}

// calculate the md5 integer hash of the given file
int md5sum(char * filename){
    char command[100];
    char buffer[33];
    FILE* fp;

    sprintf(command, "md5sum %s", filename);

    fp = popen(command, "r");
    if (fp == NULL) {
        perror("popen failed");
        exit(1);
    }

    // read the output into the buffer
    if (fgets(buffer, 33, fp) != NULL) {
        // remove the newline character at the end of the buffer
        buffer[strcspn(buffer, "\n")] = '\0';
    }
    // close the pipe
    pclose(fp);
    int sum = 0;
    for(int i = 0; buffer[i] != '\0'; i++){
        sum += (int) buffer[i];
    }
    return sum;

}

// take string timestamp convert tp double for comparisons
long double convert_timestamp(char * time_stamp){
    long double ret;
    char tmp[20]; bzero(tmp,20); strcat(tmp,time_stamp);
    char * tok = strtok(tmp,":");
    long double seconds = atoi(tok);
    tok = strtok(NULL,"\0");
    long double micro_seconds = atoi(tok);
    micro_seconds =  micro_seconds / 1000000;
    ret = seconds + micro_seconds;
    return ret;
}

// creates client socket to specified server address
int create_socket(char * server_addr){
    char addrcp[30]; bzero(addrcp,30);
    char * ip;
    char * port;
    
    strcpy(addrcp,server_addr);
    ip = strtok(addrcp,":");
    port = strtok(NULL," ");

    struct addrinfo hints, *res;
    int sockfd;

    // first, load up address structs with getaddrinfo():
    memset(&hints, 0, sizeof hints);
    hints.ai_family = AF_UNSPEC;
    hints.ai_socktype = SOCK_STREAM;

    getaddrinfo(ip,port, &hints, &res);

    // make a socket:
    sockfd = socket(res->ai_family, res->ai_socktype, res->ai_protocol);

    // connect!  // if connect fails the server is down 
    if(connect(sockfd, res->ai_addr, res->ai_addrlen) == -1){
        return -1;
    }

    return sockfd;
}

// check file struct if all files are available
int file_complete(struct srv_file_struct * f){
    for(int i = 0; i < SERVERCOUNT; i++){
        if(f -> chunkcount[i] == 0)
            return -1;
    }
    return 0;
}

// development print statement for seeing file struct contents
int print_srv_file(struct srv_file_struct* f){
    printf("=================\n");
    printf("filename:  %s\n",f->filename);
    printf("timestamp: %s\n",f->time_stamp);
    for(int i = 0; i < SERVERCOUNT; i ++){
        printf("chunk%d:", i+1);
        if(f->chunkcount[i] == 1){
            printf("present on server: %s\n",f -> chunk_tracker[i]);
        }else{
            printf("nope\n");
        }
    }
    
}
// parse original filename time_stamp and chunk number from server filename
int get_file_elements(char * full_filename, char* og_filename, char* time_stamp, int * chunk){
    int left, right, len,stamp_len;
    for(left = 0; left < strlen(full_filename); left++){
        if(full_filename[left] == '.')
            break;
    }
    for(right = strlen(full_filename); right > 0; right--){
        if(full_filename[right] == '.')
            break;
    }
    len = right - left - 1;
    stamp_len = strlen(full_filename) - right;
    memcpy(og_filename, full_filename+left+1, len);
    og_filename[len] = '\0';
    memcpy(time_stamp,full_filename+right+1,stamp_len);
    time_stamp[stamp_len] = '\0';

    char num = full_filename[0];
    *chunk = num - '0';

}

// returns file with older timestamp
struct srv_file_struct* compare_time_stamps(struct srv_file_struct * f1, struct srv_file_struct * f2){
    if(convert_timestamp(f1 -> time_stamp) >= convert_timestamp(f2 -> time_stamp))
        return f1;
    return f2;
}

// find the insert point in the linked list for new insertions
struct srv_file_struct * insert_point(struct srv_file_struct * root){
    struct srv_file_struct * index = root;
    while(index -> next_file != NULL){
        index = index -> next_file;
    }
    return index;
}

// check to see if a file is already in the linked list
struct srv_file_struct * check_if_file_exists(char * og_filename, char * time_stamp, struct srv_file_struct * root){
    while(root != NULL){
        // check if file struct exists and has same timestamp 
        if(strcmp(root -> filename, og_filename) == 0 && strcmp(root -> time_stamp,time_stamp) == 0){
            return root;
        }       
        root = root -> next_file;
    }
    return NULL;
}

// method to set ignore flag on files with the same name 
int compare_files(struct srv_file_struct * file1, struct srv_file_struct * file2){
    if(file1 == file2)
        return 0;
    // ignore files that are not the same filename
    if(strcmp(file1 -> filename,file2 -> filename) != 0)
        return 0;

    // find out which file is newer and which on is older
    struct srv_file_struct* older_file;
    struct srv_file_struct* newer_file = compare_time_stamps(file1,file2);
    if(newer_file == file1)
        older_file = file2;
    if(newer_file == file2)
        older_file = file1;
    // check completeness of both files
    int new_cplt = file_complete(newer_file);
    int old_cplt = file_complete(older_file);

    // if neither are complete ignore the older one
    if(new_cplt == -1 && old_cplt == -1){
        older_file -> ignore = 1;
    }
    //if the newer file is complete ignore the older one
    if(new_cplt == 0)
        older_file -> ignore = 1;
    // if the newer file is incomplete but the older one is ignore the newer one
    if(new_cplt == -1 && old_cplt == 0)
        newer_file -> ignore = 1;
}


// create a new server file node
struct srv_file_struct* create_new_srv_file(){
    struct srv_file_struct * new = (struct srv_file_struct *) malloc(sizeof(struct srv_file_struct));
    bzero(new ->filename,100);
    bzero(new -> time_stamp,30);
    new -> ignore = 0;
    new -> next_file = NULL;
    new -> dbl_time_stamp = 0;
    for(int i = 0; i < SERVERCOUNT; i++){
        new -> chunkcount[i] = 0;
        bzero(new -> chunk_tracker[i],20); 
    }
    return new;
}

// find duplicate name files and chose to ignore old ones
struct srv_file_struct * remove_old_copies(struct srv_file_struct * root){
    struct srv_file_struct * file1 = root;
    struct srv_file_struct * file2;
    long double max_time_stamp = 0;
    // go through all found files
    while(file1 != NULL){
        // compare against all other files
        file2 = root;
        while(file2 != NULL){
            compare_files(file1,file2);
            file2 = file2 -> next_file;
        }   
        file1 = file1 -> next_file;
    }
}

// frees linked list after use
int free_list(struct srv_file_struct * root){
    while(root != NULL){
        struct srv_file_struct * tmp = root -> next_file;
        free(root);
        root = tmp;
    }

}

// retreive available files in 
struct srv_file_struct * get_files(int server_count,char ** server_addr){
    struct srv_file_struct * root = NULL;
    char buf[3000]; bzero(buf,3000);
    int client_sock;

    for(int i = 0; i < server_count; i++){
        if((client_sock = create_socket(server_addr[i])) > 0){
            
            // retrieve buffer of all file names on server
            send(client_sock,"ls",3,0);
            recv(client_sock,buf,3000,0);

            // parse out each individual file name and populate linked list
            char * tok = strtok(buf," ");
            while(tok != NULL){
                int chunk_num;
                char og_filename[strlen(tok)];
                char time_stamp [strlen(tok)];
                get_file_elements(tok,og_filename,time_stamp,&chunk_num);

                //printf("|%s|%s|%s|%d\n",tok,og_filename,time_stamp,chunk_num);

                // check if the file has been seen before if so track the chunk
                struct srv_file_struct * file;
                if((file = check_if_file_exists(og_filename, time_stamp,root))){
                    file -> chunkcount[chunk_num-1] = 1;
                    memcpy(file -> chunk_tracker[chunk_num-1],server_addr[i],strlen(server_addr[i])); 
    
                }else{
                    // if file doesnt exist insert it and mark found chunk
                    struct srv_file_struct * newfile = create_new_srv_file();
                    strcat(newfile -> filename,og_filename);
                    strcat(newfile -> time_stamp,time_stamp);
                    memcpy(newfile -> chunk_tracker[chunk_num-1],server_addr[i],strlen(server_addr[i])); 
                    newfile -> chunkcount[chunk_num-1] = 1;

                    if(root == NULL){
                        root = newfile;
                    }else{
                        insert_point(root) -> next_file = newfile;
                    }
                }
                tok = strtok(NULL," ");
            }
            bzero(buf,3000);
        }
    }

    return root;
    
}

//======================================================================================

int put(char * path,int server_count, char ** server_addr){

    char filename[300]; bzero(filename,300); 
    parse_fname_from_path(path,filename);

    // should turn this into a function
    int hash_sum = md5sum(path);
    int bucket = hash_sum % server_count;
    int schema[server_count][2];

    for(int count = 0, i = bucket; count < server_count; count++, i++){   // calculate schema for chunk locations
        int a = i;
        if(i > server_count-1){a = i % (server_count);}
        schema[a][0] = count+1;
        schema[a][1] = count+2;
        if(count == server_count -1){
            schema[a][0] = server_count;
            schema[a][1] = 1; 
        }
    }

    struct timeval tv;
    struct timezone tz;
    struct tm * today;
    gettimeofday(&tv,&tz);
    today = localtime(&tv.tv_sec);

    size_t file_size = fsize(path);
    size_t chunk_size = file_size/server_count;

    // go through each chunk and send to respective servers
    for(int i = 1; i <= server_count; i++){
        
        //create command string to let server know what operation to conduct
        if(i == server_count){
            chunk_size = chunk_size + (file_size % server_count);
            //printf("chunk size:%lu",chunk_size);
        }
        char cmd_str[1000]; bzero(cmd_str,1000);
        char chunk[3]; sprintf(chunk,"%d",i); 
        char time_str[100]; sprintf(time_str,"%li:%li",tv.tv_sec,tv.tv_usec);
        sprintf(cmd_str,"put %s.%s.%s %lu",chunk,filename,time_str,chunk_size);

        printf("%s\n",cmd_str);

        // open file for chunkuing 
        int fd = open(path,O_RDONLY);

        if(fd < 0){
            printf("open failed\n");
        }

        // check schema for which servers to send chunk i to
        for(int a = 0; a < server_count; a++){
            if(schema[a][0] == i || schema[a][1] == i){
                //printf("send to server %d\n",a+1);
                off_t index = (chunk_size * (i-1));
                if(i == server_count)
                    index = file_size - chunk_size;
                int server_sock = create_socket(server_addr[a]);

                // send command and receive ack
                send(server_sock,cmd_str,strlen(cmd_str),0);
                char ack[3]; recv(server_sock,ack,3,0);

                // send the file chunk if ack received
                if(strcmp("ok",ack) == 0){
                    sendfile(server_sock,fd, &index,chunk_size);
                    close(server_sock);
                }else{
                    printf("no ack received\n");
                }
            }
        }

        close(fd);

    }
}

//======================================================================================


int get(char * filename,int server_count, char ** server_addr){
    // start by getting all available files and ignore old dulicates
    struct srv_file_struct* file_list_root = get_files(server_count,server_addr);
    struct srv_file_struct* index = file_list_root;
    struct srv_file_struct* target_file = NULL;
    remove_old_copies(file_list_root);

    while(index != NULL){
        if(strcmp(index -> filename,filename) == 0 && index-> ignore != 1){
            target_file = index;
            break;
        }
        index = index -> next_file;
    }

    // if there are no matches exit
    if(target_file == NULL){
        printf("%s doesn't exist\n",filename);
        return -1;
    }
    // if the file is incomplete
    if(file_complete(target_file) == -1){
        printf("%s is incomplete\n",target_file -> filename);
        return -1;
    }
    
    int fd = open(filename,O_WRONLY|O_CREAT,0666);

    // the file is present and complete reassemble
    for(int i = 0; i < server_count; i ++){
        // connect to server with chunk i+1
        int sock = create_socket(target_file -> chunk_tracker[i]); 
        char cmd[300]; bzero(cmd,300);
        char buf[BUFSIZE]; bzero(buf,BUFSIZE);
        sprintf(cmd,"get %d.%s.%s",i+1,target_file -> filename, target_file -> time_stamp);
        
        send(sock,cmd,strlen(cmd),0);
        recv(sock,buf,200,0);
        char * str_fsize = strtok(buf,".");
        int bytes_left = atoi(str_fsize);
        send(sock,"ok",3,0);

        bzero(buf,BUFSIZE);

        while(bytes_left > 0){
            int bytes_received = recv(sock,buf,BUFSIZE,0);
            write(fd,buf,bytes_received);
            bytes_left = bytes_left - bytes_received;
            bzero(buf,BUFSIZE);

        }
        close(sock);
    }
    close(fd);

    free_list(file_list_root);


}

//======================================================================================


int list(int server_count, char ** server_addr){
    struct srv_file_struct * file_list_root = get_files(server_count,server_addr);
    struct srv_file_struct * index = file_list_root;

    /* developer print statements
    while(index != NULL){
        print_srv_file(index);
        index = index -> next_file;
    }*/

    remove_old_copies(file_list_root);

    index = file_list_root;
    while(index != NULL){    
        if(index -> ignore){
            index = index -> next_file;
            continue;    
        }
        printf("%s",index -> filename);
        if(file_complete(index) == -1)
            printf(" [incomplete]");
        printf("\n");

        index = index -> next_file;
    }

    free_list(file_list_root);

}

//======================================================================================



int main(int argc , char *argv[]){

    char * command = argv[1];
    char * files[argc][100];
    char * server_addr[SERVERCOUNT];
    
    char *config = getenv("HOME");
    strcat(config,"/dfc.conf");
    FILE* fp = fopen(config,"r");
    if (fp == NULL) {
      printf("Failed to open file\n");
      return 1;
    }

    // get the servers from config file
    for(int i = 0; i < SERVERCOUNT; i++){
        size_t nread;
        size_t len = 0;
        char* buf = NULL;
        nread = getline(&buf,&len,fp);
        server_addr[i] = malloc(nread+1* sizeof(char));
        memcpy(server_addr[i],buf+12,15);
        free(buf);
    }

    // put mentioned files in an array
    for(int i = 2; i < argc && i < 20; i ++){
        files[i][0] = argv[i];
    }


    // put selected
    if(strcmp(command,"put") == 0){
        int sock_fd[SERVERCOUNT];

        // check that the file exists locally
        for(int i = 2; i < argc && i < 20; i ++){
            if (access(files[i][0], F_OK) != 0) {
                printf("%s is not accessible please provide an accessible file\n",files[i][0]);
                return -1;
            }
        }
        
        // check if all servers are up if any are down exit
        for(int i = 0; i < SERVERCOUNT; i++){
            int new_sock;
            if((new_sock = create_socket(server_addr[i])) == -1){
                printf("%s put failed\n",files[2][0]);
                close(new_sock);
                return -1;
            }else{
                close(new_sock);
            }
        }

        // put the files 
        for(int i = 2; i < argc && i < 20; i ++){
             put(files[i][0],SERVERCOUNT,server_addr);
        }

        
    }

    // list command
    if(strcmp(command,"list") == 0){
        list(SERVERCOUNT,server_addr);
    }

    // get command
    if(strcmp(command,"get") == 0){
        get(files[2][0],SERVERCOUNT,server_addr);
    }



    for(int i = 0; i < SERVERCOUNT; i++){
        free(server_addr[i]);
    }


    return 0;
}