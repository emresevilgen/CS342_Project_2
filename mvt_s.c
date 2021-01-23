#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <string.h>
#include <pthread.h>
#include <sys/file.h>
#include <sys/wait.h> 
#include <sys/time.h>
#include <semaphore.h>

struct line {
	int row;
	int column;
	int value;
};

struct semaphore {
  sem_t sem_full; // For the number of items in the buffer
	sem_t sem_empty; // For the empty slots int the buffer
};

int findNumberOfLinesInFile(char* filePath);
void splitFile();
void readVectorFile();
void mapper(char* splitFilePath, int mapperNo);
void* mapperRunner (void* k);
void reduce(char* resultfile);
void* reduceRunner ();
void deleteIntermediateFiles();

int n;
int b;
int k;
int l;
int s;
int* vector;
int* result;
char* vectorfile;
char* resultfile;
char* matrixfile;
struct line** buffers; // All buffers
struct semaphore* sems; // Semaphore array for each buffer
sem_t mutex; // Make the reducer wait


int main(int argc, char* argv[]) {
  // Check the inputs
  if (argc != 6){
    printf("Inputs should be in the following format: mvt_s matrixfile vectorfile resultfile K B\n");
    exit(0);
  }

  // Get the inputs
  matrixfile = argv[1];
  vectorfile = argv[2];
  resultfile = argv[3];
  k = atoi(argv[4]);
  b = atoi(argv[5]);

  // Check the boundries
  if (k <= 0){
    printf("K should be greater than zero.\n");
    exit(0);
  }

    if (b <= 0){
    printf("B should be greater than 0.\n");
    exit(0);
  }

  if (matrixfile)

  // Create buffer and semaphore arrays
  buffers = malloc(k * sizeof(struct line*));
  sems = malloc(k * sizeof(struct semaphore));

  // Initilize the semaphores and buffers
  for (int i = 0; i < k; ++i){
    buffers[i] = malloc(b * sizeof(struct line));
    sem_init(&(sems[i].sem_empty), 0, b);
    sem_init(&(sems[i].sem_full), 0, 0);
  }

  sem_init(&mutex, 0, 0);

  // Calculate the number of lines in both vectorfile and matrixfile 
  n = findNumberOfLinesInFile(vectorfile);
  l = findNumberOfLinesInFile(matrixfile);

  // Create the vector and result array, then read the vectorfile
  vector = malloc(n * sizeof(int));
  result = malloc(n * sizeof(int));
  readVectorFile();

  // Calculate the line number for splits and split the file
  s = l/k;
  splitFile();

  // Create the mapper threads
  pthread_t tid[k]; 
  pthread_t tid_c; 

  pthread_attr_t attr; 
  char temp[k][15];

  for(int i = 1; i <= k; ++i){
      sprintf(temp[i-1], "%d", i);
      pthread_attr_init( &attr);
      pthread_create(&(tid[i-1]), &attr, mapperRunner, temp[i-1]);
  }

  // Create reducer thread
  char count[15];
  sprintf(count, "%d", k);
  pthread_attr_init( &attr);
  pthread_create(&tid_c, &attr, reduceRunner, count);

  // Join threads
  for(int i = 0; i < k; ++i)
    pthread_join(tid[i], NULL);

  pthread_join(tid_c, NULL);


  // Clean up
  deleteIntermediateFiles();
  free(vector);
  free(result);
  for (int i = 0; i < k; i++){
   free(buffers[i]);
  }
  free(buffers);
  free(sems);

  return 0;
}

int findNumberOfLinesInFile(char* filePath) {
  // Open file
  FILE* fp = fopen(filePath, "r");

  if(fp == NULL){
    printf("Could not open the %s\n", filePath);
    exit(0);
  }
  
  // Count the lines
  int l = 0;
  char ch;

  while(!feof(fp)){
    ch = fgetc(fp);
    if(ch == '\n') {
      ++l;
    }
  }

  fclose(fp);
  return l;
}

void splitFile() {
  // Open file
  FILE* fp_in = fopen (matrixfile, "r");

  if(fp_in == NULL) {
    printf("Could not open the %s %s", matrixfile, "\n");
    exit(0);
  }

  FILE* fp_out;
  
  char * line = NULL;
  size_t len = 0;
  ssize_t read;  
  
  char outputFilename[15];
  int filecounter = 0; 
  int linecounter = 0;
  int rem = l % k; // For the last file

  // Get each line
  while (linecounter < l) {
    read = getline(&line, &len, fp_in);
    // Check end of file
    if (read == -1) {
      break;
    }
    // Change file
    if (linecounter % s == 0) {
      if (linecounter + rem < l) { // For the last file
        // Close file
        if (linecounter != 0)
          fclose(fp_out);
        // Generate output filename and open the file
        filecounter++;
        sprintf(outputFilename, "split%d", filecounter);
        fp_out = fopen(outputFilename, "w");
        if (fp_out == NULL){
          printf("Could not write to the %s %s", outputFilename, "\n");
          exit(0);
        }
      }
    }
    // Write the line
    fwrite(line, read, 1, fp_out);
    linecounter++;
  }

  // Close files and free line
  free(line);
  fclose(fp_in);
  fclose(fp_out);
}

void readVectorFile() {
  // Open the file
  FILE* fp_in = fopen (vectorfile, "r");

  if(fp_in == NULL) {
    printf("Could not open the %s %s", vectorfile, "\n");
    exit(0);
  }
  
  char * line = NULL;
  size_t len = 0;
  ssize_t read;  

  // Read each line
  for(int i = 0; i < n ; i++) {
    read = getline(&line, &len, fp_in);
    if (read == -1)
      break;
    
    // Split the line with space and put the value into vector array
    char delim[] = " ";
    char* ptr = strtok(line, delim);
    int index = atoi(ptr);
    int value = atoi(strtok(NULL, delim));
    vector[index - 1] = value;
  }

  // Close file and free line
  free(line);
  fclose(fp_in);
}

void mapper(char* splitFilePath, int mapperNo) {
  // Open the file
  FILE* fp_in = fopen(splitFilePath, "r");
  if(fp_in == NULL) {
    printf("Could not open the %s %s", splitFilePath, "\n");
    return;
  }

  char * line = NULL;
  size_t len = 0;
  ssize_t read;  
  int index = 0; // Index of the buffer empty slot

  // Read each line from the file
  while ((read = getline(&line, &len, fp_in)) != -1) {
    // Get line attribures
    char * end;
    int row = strtol(line, &end, 10);
    int col = strtol(end, &end, 10);
    int value = strtol(end, &end, 10);

    // Wait for empty buffer slot
    sem_wait(&(sems[mapperNo-1].sem_empty));

    // Put the line into buffer
    buffers[mapperNo-1][index].column = col;
    buffers[mapperNo-1][index].row = row;
    buffers[mapperNo-1][index].value = value;

    // Increase the buffer index circulary
    index++;
    index = index % b;

    // Signal the full semaphore and semaphore for reducer
    sem_post(&(sems[mapperNo-1].sem_full));
    sem_post(&mutex);
  }

  // Close file and free line
  free(line);
  fclose(fp_in);
}

void* mapperRunner (void* k) {
  // Generate splitfile name and call mapper function
  int n = atoi(k);
  char splitFilepath[15];
  snprintf(splitFilepath, 15, "split%d",  n);
  mapper(splitFilepath, n);
  pthread_exit(0);
}

void reduce(char* resultfile){


  // Initilize the result array
  for(int i = 0; i < n; ++i)
    result[i] = 0;

  int indexes[k]; // Indexes for all buffers

  // Initilize the buffer indexes
  for (int i = 0; i < k; ++i) {
    indexes[i] = 0;
  }

  // Loop into the line number of the matrixfile
  for(int j = 0; j < l; ++j) {
    // Wait for the mapper signal of semaphore
    sem_wait(&mutex);

    // Initilize the first index by different values to 
    // fair iteration over buffers
    int i = j % k;

    while (1){
      int index = i;
      // Try each buffer and check if the buffer produced or not
      if (!sem_trywait(&(sems[index].sem_full))){
        // Get index of the line at the buffer
        int bufferIndex = indexes[i];

        // Get the line
        struct line consumedLine = buffers[index][bufferIndex];
        int col = consumedLine.column;
        int row = consumedLine.row;
        int val = consumedLine.value;

        // Make multiplication and add it to the result array
        result[row -1] = result[row - 1] + (val * vector[col - 1]);

        // Increment the index in circular order
        indexes[i] = (indexes[i] + 1) % b;

        // Signal to the semaphore of the empty slots in the buffer
        sem_post(&(sems[index].sem_empty));
        break;
      }
      i = (i + 1)%k;
    }
  }

  // Open the file
  FILE *fptr = fopen(resultfile, "w+");
  if(fptr == NULL)
    printf("Could not open the %s.", resultfile);
  
  // Write into the file and close the file
  for(int i = 0; i < n ; i++) {
    fprintf(fptr, "%d %d\n", i+1 , result[i]);
  }
  fclose(fptr);
}

void* reduceRunner() {
  // Call reduce funtion
  reduce(resultfile);
  pthread_exit(0);
}

void deleteIntermediateFiles(){
  int filecounter = 1;
  char outputFilename[15];

  // Generate split file names and remove them
  sprintf(outputFilename, "split%d", filecounter);
  while (remove(outputFilename) == 0){
    ++filecounter;
    sprintf(outputFilename, "split%d", filecounter);
  }
}