#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <libgen.h>
#include <dirent.h>
#include <unistd.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <errno.h>
#include <fcntl.h>
#include <unistd.h>
#include <pthread.h>
#include "tpool.h"

tpool_t *tm;
pthread_mutex_t  work_mutex;

typedef struct {
    char *source_path;          // file/folder source path.
    char *destination_path;     // file/folder destination path.
} thread_args;


/**
 * \brief Clearing the memory of the thread_args structure
 */
void free_thread_args(thread_args * data) {
    if (data == NULL)
        return;

    if (data->source_path != NULL)
        free(data->source_path);
    if (data->destination_path != NULL)
        free(data->destination_path);
    
    free(data);
}


/**
 * \brief Copy files from source to destination.
 * If the function was executed successfully, it outputs the tid, path, and copied bytes.
 */
void copy_file(void *data) {
    thread_args* params = (thread_args*)data;
    char* source_path = params->source_path;
    char* destination_path = params->destination_path;

    int source_file, destination_file;
    char buffer[4096];
    size_t bytes_read, bytes_written;

    source_file = open(source_path, O_RDONLY);
    if (source_file == -1) {
        fprintf(stderr,"%s: %s\n", source_path, strerror(errno)); 
        free_thread_args(params);
        return;
    }

    struct stat source_stat;
    if (lstat(source_path, &source_stat) == -1) {
        fprintf(stderr,"%s: %s\n", source_path, strerror(errno));
        close(source_file);
        free_thread_args(params);
        return;
    }
    
    destination_file = open(destination_path, O_WRONLY | O_CREAT | O_TRUNC, source_stat.st_mode);
    if (destination_file == -1) {
        fprintf(stderr,"%s: %s\n", destination_path, strerror(errno));
        close(source_file);
        free_thread_args(params);
        return;
    }
    
    size_t bytes_count = 0;
    while ((bytes_read = read(source_file, buffer, sizeof(buffer))) > 0) {
        bytes_written = write(destination_file, buffer, bytes_read);
        bytes_count += bytes_read;
        if (bytes_written == -1) {
            fprintf(stderr,"%s: %s\n", destination_path, strerror(errno));
            close(source_file);
            close(destination_file);
            free_thread_args(params);
            return;
        }
    }

    if (close(source_file) == -1)
    	fprintf(stderr,"%s: %s\n", source_path, strerror(errno));
    if (close(destination_file) == -1)
    	fprintf(stderr,"%s: %s\n", destination_path, strerror(errno));

    if (bytes_read == -1) {
        fprintf(stderr,"%s: %s\n", source_path, strerror(errno));
        remove(destination_path); 
    } else {
        printf("tid: %ld, %s, copied bytes: %lu\n", pthread_self(), source_path, bytes_count);
    }

    free_thread_args(params);
}


/**
 * \brief Create a symbolic link.
 */
void copy_link(void *data) {
    thread_args* params = (thread_args*)data;
    char* source_path = params->source_path;
    char* destination_path = params->destination_path;

    char target_path[PATH_MAX];
    ssize_t target_length = readlink(source_path, target_path, sizeof(target_path) - 1);
    if (target_length == -1) {
        fprintf(stderr,"%s: %s\n", source_path, strerror(errno));
        free_thread_args(params);
        return;
    }

    target_path[target_length] = '\0';
    pthread_mutex_lock(&work_mutex);
    char *rpath = realpath(source_path, target_path);
    pthread_mutex_unlock(&work_mutex);

    if (rpath == NULL) {
        fprintf(stderr,"%s: %s\n", source_path, strerror(errno));
        free_thread_args(params);
        return;
    }
    
    if (symlink(rpath, destination_path) == -1) {
        fprintf(stderr,"%s: %s\n", destination_path, strerror(errno));
        free_thread_args(params);
        return;
    }

    free_thread_args(params);
}


/**
 * \brief Makes a copy of the dirent structure.
 * 
 * \context Re-reading a directory is possible only if the directory is closed and opened again,
 * since there is no way to change the pointer to the beginning of the directory.
 * In order not to open and close the directory every time,
 * the information about this directory is copied once for further reusable use.
 * 
 * \return If the function completes successfully, it returns the number of entities in the directory,
 * otherwise it returns -1.
 */
int save_dir_entries(DIR *dir, struct dirent ***entries_ptr) {
    struct dirent *entry;
    int num_entries = 0;
    struct dirent **entries = NULL;

    while ((entry = readdir(dir)) != NULL) {
        if (strcmp(entry->d_name, ".") != 0 && strcmp(entry->d_name, "..") != 0) {
            num_entries++;
            entries = realloc(entries, num_entries * sizeof(struct dirent *));

            if (entries == NULL) {
                return -1;
            }

            entries[num_entries - 1] = malloc(sizeof(struct dirent));
            memcpy(entries[num_entries - 1], entry, sizeof(struct dirent));
        }
    }

    *entries_ptr = entries;  
    return num_entries;
}


/**
 * \brief Compares and copies files from the source directory to the destination directory.
 * 
 * \context The function opens directories and starts comparing their contents with each other.
 * If there is no such content in the destination directory,
 * the task of copying this information is put in threadpool.
 */
void cmp_and_exchange(const char* source_path, const char* destination_path) {

    DIR *dir1, *dir2;
    struct dirent *entry1, *entry2;
    struct dirent **entries = NULL;
    int num_entries = 0;
    
    dir1 = opendir(source_path);
    dir2 = opendir(destination_path);

    if (dir1 == NULL) {
        fprintf(stderr,"%s: %s\n", source_path, strerror(errno)); 
        return;
    }

    if (dir2 == NULL) {
        fprintf(stderr,"%s: %s\n", destination_path, strerror(errno)); 
        return;
    }
    
    if ((num_entries = save_dir_entries(dir2, &entries)) == -1) {
        fprintf(stderr,"%s: %s\n", destination_path, strerror(errno)); 
        return;
    }

    while ((entry1 = readdir(dir1)) != NULL) {
        if (strcmp(entry1->d_name, ".") == 0 || strcmp(entry1->d_name, "..") == 0)
            continue;

        bool exist = false;
        for (int i = 0; i < num_entries; i++) {    
            struct dirent *entry2 = entries[i];

            if (strcmp(entry2->d_name, ".") == 0 || strcmp(entry2->d_name, "..") == 0)
                continue;

            if (entry1->d_type == DT_DIR && entry2->d_type == DT_DIR && strcmp(entry1->d_name, entry2->d_name) == 0) {
                char pathDir1[PATH_MAX];
                char pathDir2[PATH_MAX];
                snprintf(pathDir1, sizeof(pathDir1), "%s/%s", source_path, entry1->d_name);
                snprintf(pathDir2, sizeof(pathDir2), "%s/%s", destination_path, entry2->d_name);
                cmp_and_exchange(pathDir1, pathDir2);
            }

            if (entry1->d_type == entry2->d_type && strcmp(entry1->d_name,entry2->d_name) == 0) {
                exist = true;
                break;
            }          
        }

        if (exist == false) {
            char *pathDir1 = malloc(sizeof(char) * PATH_MAX);
            char *pathDir2 = malloc(sizeof(char) * PATH_MAX);
            snprintf(pathDir1, PATH_MAX, "%s/%s", source_path, entry1->d_name);
            snprintf(pathDir2, PATH_MAX, "%s/%s", destination_path, entry1->d_name);

            thread_args* params = (thread_args*)malloc(sizeof(thread_args));
            params->source_path = pathDir1;
            params->destination_path = pathDir2;

            switch(entry1->d_type) {
                case DT_DIR:
                    struct stat pstatus;
                    int dir_create;
                    if (lstat(pathDir1, &pstatus) == -1) {
                        fprintf(stderr,"%s: %s\n", pathDir1, strerror(errno));
                        dir_create = mkdir(pathDir2, 0777);
                    } else {
                        dir_create = mkdir(pathDir2, pstatus.st_mode);
                    }

                    if (dir_create == 0) {
                        cmp_and_exchange(pathDir1, pathDir2);
                    } else {
                        fprintf(stderr,"%s: %s\n", pathDir2, strerror(errno));
                    }
                            
                    free_thread_args(params);
                    break;
                case DT_LNK: 
                    tpool_add_work(tm, copy_link, params); 
                    break;
                case DT_REG: 
                    tpool_add_work(tm, copy_file, params);
                    break;
            }
        }      
    }

    free(entries);
    closedir(dir1);
    closedir(dir2);
}    


int main(int argc, char **argv) {

    if(argc < 4){
        fprintf(stderr, "%s: ERROR:not enough arguments\n", basename(argv[0]));
        return -1;
    }

    char* programName = basename(argv[0]);
    char* dir1 = realpath(argv[1], NULL);
    char* dir2 = realpath(argv[2], NULL);
    int threadCount = atoi(argv[3]);

    if (dir1 == NULL || dir2 == NULL) {
        fprintf(stderr,"%s: %s\n", basename(argv[0]), strerror(errno));
        return -1;
    }

    pthread_mutex_init(&work_mutex, NULL);
    tm = tpool_create((size_t)threadCount);

    cmp_and_exchange(dir1, dir2);
    cmp_and_exchange(dir2, dir1);

    tpool_wait(tm);
    tpool_destroy(tm);
    pthread_mutex_destroy(&work_mutex);
    
    return 0;
}
