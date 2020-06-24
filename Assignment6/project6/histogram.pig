P = LOAD '$P' using PigStorage(',') as (RED:int,GREEN:int,BLUE:int);
R = group P by RED;
G = group P by GREEN;
B = group P by BLUE;
RR = foreach R generate 1,FLATTEN(group),COUNT($1);
GG = foreach G generate 2,FLATTEN(group),COUNT($1);
BB = foreach B generate 3,FLATTEN(group),COUNT($1);
RES = UNION RR,GG,BB;
store RES into '$O' using PigStorage(' ');