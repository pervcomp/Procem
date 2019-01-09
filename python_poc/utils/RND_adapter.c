// Copyright (c) TUT Tampere University of Technology 2015-2018.
// This software has been developed in Procem-project funded by Business Finland.
// This code is licensed under the MIT license.
// See the LICENSE.txt in the project root for the license terms.
//
// Main author(s): Ville Heikkila, Otto Hylli, Pekka Itavuo,
//                 Teemu Laukkarinen ja Ulla-Talvikki Virta


#include <stdio.h>
#include <time.h>
#include <stdint.h>
#include <pthread.h>
#include <errno.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <netinet/in.h>
#include <unistd.h> /* for close() for socket */
#include <stdlib.h>



int send_UDP_Packet(unsigned char* ip, uint16_t port, uint8_t* msg, uint32_t msg_size){
  struct sockaddr_in sa;
  int s, bytes;

  s = socket(PF_INET, SOCK_DGRAM, IPPROTO_UDP);
  if(s == -1){
    // fail, return
    return -1;
  }

  // ipv4 address
  sa.sin_family = AF_INET;
  // target addr
  sa.sin_addr.s_addr = inet_addr(ip);
  // port
  sa.sin_port = htons(port);

  bytes = sendto(s, msg, msg_size, 0, (struct sockaddr*)&sa, sizeof(sa));

  if( bytes < 0 || bytes != msg_size){
    return -1;
  } else {
    return 0;
  }
}

const unsigned char* IOTTicketPKT = "{\"name\": \"RandomNumber\", \"path\": \"ProCem/Core\", \"v\": %d, \"ts\": %d, \"unit\": \"Num\"}";

int main(){
  while(0 < 1){
    uint8_t* buf = malloc(sizeof(IOTTicketPKT)+128);
    uint32_t pkt_size = snprintf(buf, sizeof(IOTTicketPKT)+128, IOTTicketPKT, rand(), (unsigned int)time(NULL));
    if(pkt_size < sizeof(IOTTicketPKT)+128 ){
      printf(buf); printf("\n\r"); // debug
      send_UDP_Packet("127.0.0.1", 6666, buf, pkt_size);
    } else {
      perror("Run out of space in buffer\n\r");
    }
    free(buf);
    usleep(1000000);
  }
}
