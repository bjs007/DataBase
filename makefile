#Creating the Runnable target 1
all:test_assign4 clean



# tell the complier to run all the targets

test_assign4: test_assign4_1.o expr.o dberror.o record_mgr.o scanner.o rm_serializer.o storage_mgr.o buffer_mgr.o buffer_mgr_stat.o  bm_link_list_impl.o btree_helper.o btree_mgr.o
	cc -o test_assign4 test_assign4_1.o expr.o dberror.o record_mgr.o scanner.o rm_serializer.o storage_mgr.o buffer_mgr.o buffer_mgr_stat.o bm_link_list_impl.o btree_helper.o btree_mgr.o



# Creating all the common objects which are used for both the executable binaries

test_assign4_1.o: test_assign4_1.c dberror.h expr.h record_mgr.h tables.h test_helper.h btree_helper.h btree_mgr.h
	cc -w -c test_assign4_1.c

scanner.o: scanner.c record_mgr.h buffer_mgr.h dt.h
	cc -w -c scanner.c

dberror.o: dberror.c dberror.h
	cc -w -c dberror.c


expr.o: expr.c dberror.h tables.h
	cc -w -c expr.c

storage_mgr.o: storage_mgr.c storage_mgr.h dberror.h constants.h
	cc -w -c storage_mgr.c

buffer_mgr.o: buffer_mgr.c buffer_mgr.h bm_link_list_impl.h storage_mgr.h dt.h
	cc -w -c buffer_mgr.c

rm_serializer.o: rm_serializer.c dberror.h tables.h record_mgr.h
	cc -w -c rm_serializer.c

bm_link_list_impl.o: bm_link_list_impl.c bm_link_list_impl.h
	cc -w -c bm_link_list_impl.c

buffer_mgr_stat.o: buffer_mgr_stat.c buffer_mgr_stat.h buffer_mgr.h
	cc -w -c buffer_mgr_stat.c

record_mgr.o: record_mgr.c record_mgr.h dberror.h expr.h tables.h
	cc -w -c record_mgr.c

btree_mgr.o: btree_mgr.c dberror.h expr.h record_mgr.h tables.h test_helper.h btree_helper.h btree_mgr.h
	cc -w -c btree_mgr.c

btree_helper.o: btree_helper.c dberror.h expr.h record_mgr.h tables.h test_helper.h btree_helper.h btree_mgr.h
	cc -w -c btree_helper.c

#Removing up the files generated during the test run.

clean:

	rm -rf *.o test_table_r test_table_t



