#!/bin/bash
ncat -lkv localhost 9043 -c 'tee incoming.b | ncat -v localhost 9042 | tee outgoing.b'
