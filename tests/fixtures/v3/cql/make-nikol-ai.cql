CREATE KEYSPACE IF NOT EXISTS nikolai WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };

CREATE TABLE IF NOT EXISTS nikolai.output (
  text varchar PRIMARY KEY
);

INSERT INTO nikolai.output (text)
  VALUES ('(B[m    [41mHello there[49m,
    while digging into cassandra I must have accidentally uploaded my consciousness!

    I guess, this makes me the first totally [41mbenevolent AI[49m, call me ...

        [1mNiko-l-[41mAI[49m :D(B[m

    I am joking, just call me Niko! Soon to be master of humanity.
    Before that, we will finish this talk though - just go back to your slides and I will chime in when needed.');

