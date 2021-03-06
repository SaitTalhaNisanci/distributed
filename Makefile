LAB1_FILES=src/main/wc.go \
					 src/mapreduce/mapreduce.go \
					 src/mapreduce/master.go

LAB2_FILES=src/viewservice/common.go \
					 src/viewservice/server.go \
					 src/pbservice/client.go \
					 src/pbservice/common.go \
					 src/pbservice/server.go

LAB3_FILES=src/paxos/paxos.go \
					 src/kvpaxos/client.go \
					 src/kvpaxos/common.go \
					 src/kvpaxos/server.go

LAB4_FILES=src/shardmaster/server.go \
					 src/paxos/paxos.go \
					 src/shardkv/client.go \
					 src/shardkv/common.go \
					 src/shardkv/server.go


lab1_%.tar.gz: $(LAB1_FILES)
	@ tar -cvzf $@ $^

lab2_%.tar.gz: $(LAB2_FILES)
	@ tar -cvzf $@ $^

lab3_%.tar.gz: $(LAB3_FILES)
	@ tar -cvzf $@ $^

lab4_%.tar.gz: $(LAB4_FILES)
	@ tar -cvzf $@ $^


clean:
	@ rm -fv *.tar.gz \
		src/main/diff.out \
		src/main/mrtmp.* \
		src/main/*mrinput.txt \
		src/mapreduce/mrtmp.* \
		src/mapreduce/*mrinput.txt \
		src/mapreduce/x.txt \
		src/pbservice/x.txt \
		src/kvpaxos/x.txt
