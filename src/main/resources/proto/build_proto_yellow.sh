#! /bin/bash
rm -rf YellowProto.java
protoc --java_out=./ Yellow.proto
