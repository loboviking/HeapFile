Pehr Collins (Heapfile)
11/1/2007

A heap file is an unordered set of records. The following operations are supported: 
* Heap files can be created and destroyed. 
* Existing heapfiles can be opened and closed.
* Records can be inserted and deleted. 
* Records are uniquely identified by a record id (rid). A specific record can be retrieved by using the record id. 

Overall Status:

I implemented the stubbed methods in HeapFile.Java.  I started with the constructor then I worked mostly from the bottom up,
since understanding the helper methods would allow me to better understand the methods that would call
them and how they would call them.  I also modified DirPage.Java slightly by giving it an accessor for
MaxEntries.  I use this to help determine if a directory page is full or not in InsertPage.  The 
project is now complete and it passes all tests.