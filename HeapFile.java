package heap; 

import global.GlobalConst;
import global.Minibase;
import global.PageId;
import global.RID;

/**
 * <h3>Minibase Heap Files</h3>
 * A heap file is the simplest database file structure.  It is an unordered 
 * set of records, stored on a set of data pages. <br>
 * This class supports inserting, selecting, updating, and deleting
 * records.<br>
 * Normally each heap file has an entry in the database's file library.
 * Temporary heap files are used for external sorting and in other
 * relational operators. A temporary heap file does not have an entry in the
 * file library and is deleted when there are no more references to it. <br>
 * A sequential scan of a heap file (via the HeapScan class)
 * is the most basic access method.
 */
public class HeapFile implements GlobalConst {

  /** HFPage type for directory pages. */
  protected static final short DIR_PAGE = 10;

  /** HFPage type for data pages. */
  protected static final short DATA_PAGE = 11;

  // --------------------------------------------------------------------------

  /** Is this a temporary heap file, meaning it has no entry in the library? */
  protected boolean isTemp;

  /** The heap file name.  Null if a temp file, otherwise 
   * used for the file library entry. 
   */
  protected String fileName;

  /** First page of the directory for this heap file. */
  protected PageId headId;

  // --------------------------------------------------------------------------

  /**
   * If the given name is in the library, this opens the corresponding
   * heapfile; otherwise, this creates a new empty heapfile. 
   * A null name produces a temporary file which
   * requires no file library entry.
   */
  public HeapFile(String name) {
	  
	  fileName = name;

	  if (null != name)
	  {
		  isTemp = false;
		  PageId pageId = Minibase.DiskManager.get_file_entry(name); 
		  
		  if (null != pageId)
		  {
			  headId = pageId;
		  }
		  else
		  {
			  CreateEmptyHeapFile();
		  }
	  }
	  else // Temporary heapfile
	  {
		  isTemp = true;
		  CreateEmptyHeapFile();
	  }

  } // public HeapFile(String name)

  /**
   * Called by the garbage collector when there are no more references to the
   * object; deletes the heap file if it's temporary.
   */
  protected void finalize() throws Throwable {

	  if (isTemp)
	  {
		  deleteFile();
	  }

  } // protected void finalize() throws Throwable

  /**
   * Deletes the heap file from the database, freeing all of its pages
   * and its library entry if appropriate.
   */
  public void deleteFile() {
	  
	  PageId nextDirectoryPageId = new PageId();
	  DirPage directoryPage = new DirPage();
	  
	  // Read the head directory page
	  Minibase.BufferManager.pinPage(headId, directoryPage, PIN_DISKIO); 
	  
	  do 
	  {
		  // Hit every data page referenced by this directory page and free that page
		  for (int i = 0; i < directoryPage.getEntryCnt(); i++)
		  {
			  // Free the page
			  Minibase.BufferManager.freePage(directoryPage.getPageId(i));
		  }
		  // We have freed all the data pages referenced by this page
		  // so free this directory page and move to the next directory page
		  nextDirectoryPageId = directoryPage.getNextPage();
	      Minibase.BufferManager.unpinPage(directoryPage.getCurPage(), UNPIN_CLEAN);
	      Minibase.BufferManager.freePage(directoryPage.getCurPage());
		  
		  if (INVALID_PAGEID != nextDirectoryPageId.pid) 
		  {
		      // pin the next directory page
			  // for the next pass through the loop
		      Minibase.BufferManager.pinPage(nextDirectoryPageId, directoryPage, PIN_DISKIO);
		  }
	  } while (INVALID_PAGEID != nextDirectoryPageId.pid);

  } // public void deleteFile()

  /**
   * Inserts a new record into the file and returns its RID.
   * Should be efficient about finding space for the record.
   * However, fixed length records inserted into an empty file
   * should be inserted sequentially.
   * Should create a new directory and/or data page only if
   * necessary.
   * 
   * @throws IllegalArgumentException if the record is too 
   * large to fit on one data page
   */
  public RID insertRecord(byte[] record) throws IllegalArgumentException {
	  
	  HFPage dataPage = new HFPage();
	  
	  // Find a page with sufficient free space for this record
	  // Will throw IllegalArgumentException if record is too large to fit on one data page
	  PageId dataPageId = getAvailPage(record.length);
	  
	  // Pin the target data page
	  Minibase.BufferManager.pinPage(dataPageId, dataPage, PIN_DISKIO); 
	  
	  // Insert the record into the data page
	  RID recordId = dataPage.insertRecord(record);
	  
	  // Update the entry to the directory for this record
	  updateDirEntry(dataPageId, 1, dataPage.getFreeSpace());
	  
	  // Unpin (save) the data page
      Minibase.BufferManager.unpinPage(dataPageId, UNPIN_DIRTY);
      
      return recordId;
	  
	  
	
   } // public RID insertRecord(byte[] record)

  /**
   * Reads a record from the file, given its rid.
   * 
   * @throws IllegalArgumentException if the rid is invalid
   */
  public byte[] selectRecord(RID rid) throws IllegalArgumentException {
	  
	  HFPage dataPage = new HFPage();
	  byte[] record;
	  
	  // Pin the data page so that we can select the record 
      Minibase.BufferManager.pinPage(rid.pageno, dataPage, PIN_DISKIO);
      
      // Will throw IllegalArgumentException if the rid is invalid
      record = dataPage.selectRecord(rid).clone();
      
      // Unpin the data page to since we are done with it
      Minibase.BufferManager.unpinPage(rid.pageno, UNPIN_CLEAN);

      return record;
  } // public byte[] selectRecord(RID rid)

  /**
   * Updates the specified record in the heap file.
   * 
   * @throws IllegalArgumentException if the rid or new record is invalid
   */
  public void updateRecord(RID rid, byte[] newRecord) throws IllegalArgumentException {

	  HFPage dataPage = new HFPage();
	  
	  // Pin the data page so that we can update the record
      Minibase.BufferManager.pinPage(rid.pageno, dataPage, PIN_DISKIO);
	  
	  // Update the record with the newRecord
	  // Will throw IllegalArgumentException if the rid is invalid or input record's length is different
	  dataPage.updateRecord(rid, newRecord);
	  
	  // Unpin the data page to save the changes
      Minibase.BufferManager.unpinPage(rid.pageno, UNPIN_DIRTY);

  } // public void updateRecord(RID rid, byte[] newRecord)

  /**
   * Deletes the specified record from the heap file.
   * Removes empty data and/or directory pages.
   * 
   * @throws IllegalArgumentException if the rid is invalid
   */
  public void deleteRecord(RID rid) throws IllegalArgumentException {
	  
	  HFPage dataPage = new HFPage();
	  
	  // Pin the data page so that we can delete the record and check the rid
      Minibase.BufferManager.pinPage(rid.pageno, dataPage, PIN_DISKIO);
	   
	  // Delete the record from the data page
      // Will throw IllegalArgumentException if the rid is invalid
      dataPage.deleteRecord(rid);	
      
      // Save our freespace to pass to update the directory
      int freeSpace = dataPage.getFreeSpace();
      
      // Unpin this dataPage to save the changes
      Minibase.BufferManager.unpinPage(rid.pageno, UNPIN_DIRTY);
      
      // Update our entry: 1 less record and a free count that increases by the size of the freed slot
      updateDirEntry(rid.pageno, -1, freeSpace);
      
  } // public void deleteRecord(RID rid)

  /**
   * Gets the number of records in the file.
   */
  public int getRecCnt() {

	  DirPage directoryPage = new DirPage();
	  PageId nextDirectoryPageId = new PageId();
	  int count = 0;
	  
	  // Read the head directory page
	  Minibase.BufferManager.pinPage(headId, directoryPage, PIN_DISKIO); 
	  
	  do 
	  {
		  // Count up the records on this directory page
		  for (int i = 0; i < directoryPage.getEntryCnt(); i++)
		  {
			  count = count + directoryPage.getRecCnt(i);
		  }
		  // Move on to the next directory page
		  nextDirectoryPageId = directoryPage.getNextPage();
		  
		  if (INVALID_PAGEID != nextDirectoryPageId.pid) 
		  {
		      // Unpin the current directory page, pin the next directory page
			  // for the next pass through the loop
		      Minibase.BufferManager.unpinPage(directoryPage.getCurPage(), UNPIN_CLEAN);
		      Minibase.BufferManager.pinPage(nextDirectoryPageId, directoryPage, PIN_DISKIO);
		  }
	  } while (INVALID_PAGEID != nextDirectoryPageId.pid);
	  
	  // Unpin the final directory page since we are done with it
      Minibase.BufferManager.unpinPage(directoryPage.getCurPage(), UNPIN_CLEAN);
	  
	  return count;

  } // public int getRecCnt()

  /**
   * Initiates a sequential scan of the heap file.
   */
  public HeapScan openScan() {
    return new HeapScan(this);
  }

  /**
   * Returns the name of the heap file.
   */
  public String toString() {
    return fileName;
  }

  /**
   * Searches the directory for the first data page with enough free space to store a
   * record of the given size. If no suitable page is found, this creates a new
   * data page.
   * A more efficient implementation would start with a directory page that is in the
   * buffer pool.
   * 
   * @throws IllegalArgumentException if the record is too 
   * large to fit on one data page
   */
  protected PageId getAvailPage(int reclen) {
	  
	  DirPage directoryPage = new DirPage();
	  PageId nextDirectoryPageId = new PageId();
	  
	  // Throw an exception if the reclen is greater than the page size minus the header and 1 slot
	  if (reclen > PAGE_SIZE - 20 /*header size*/ - 4 /*slot size*/)
	  {
		  throw new IllegalArgumentException("Record size exceeds page size!");
	  }
	  
	  // Read the head directory page
	  Minibase.BufferManager.pinPage(headId, directoryPage, PIN_DISKIO); 
	  
	  do 
	  {
		  // Look for the a page with enough free space to store a record of the given size
		  for (int i = 0; i < directoryPage.getEntryCnt(); i++)
		  {
			  if (directoryPage.getFreeCnt(i) >= reclen + 4 /*account for slot size*/)
			  {
				  // Cache return value before unpinning
				  PageId availablePageId = directoryPage.getPageId(i);
				  // Unpin the page to leave it the way we found it
			      Minibase.BufferManager.unpinPage(directoryPage.getCurPage(), UNPIN_CLEAN);
				  return availablePageId;
			  }
		  }
		  // No page found with sufficient space so move to the next directory page
		  nextDirectoryPageId = directoryPage.getNextPage();
		  
		  if (INVALID_PAGEID != nextDirectoryPageId.pid) 
		  {
		      // unpin the current directory page, pin the next directory page
			  // for the next pass through the loop
		      Minibase.BufferManager.unpinPage(directoryPage.getCurPage(), UNPIN_CLEAN);
		      Minibase.BufferManager.pinPage(nextDirectoryPageId, directoryPage, PIN_DISKIO);
		  }
	  } while (INVALID_PAGEID != nextDirectoryPageId.pid);
	  
	  // Unpin the directory page to leave it the way we found it
      Minibase.BufferManager.unpinPage(directoryPage.getCurPage(), UNPIN_CLEAN);
	  // If we hit this point then no page with sufficient space was found, must create
	  return insertPage();	  

  } // protected PageId getAvailPage(int reclen)

  /**
   * Helper method for finding directory entries of data pages.
   * 
   * @param pageno identifies the page for which to find an entry
   * @param dirId output param to hold the directory page's id (pinned)
   * @param dirPage output param to hold directory page contents
   * @return index of the data page's entry on the directory page
   */
  protected int findDirEntry(PageId pageno, PageId dirId, DirPage dirPage) {

	  PageId nextDirectoryPageId = new PageId();
	  
	  // Read the head directory page
	  Minibase.BufferManager.pinPage(headId, dirPage, PIN_DISKIO); 
	  
	  do 
	  {
		  // Look for the given data PageId in this directory Page
		  for (int i = 0; i < dirPage.getEntryCnt(); i++)
		  {  
			  if (pageno.pid == dirPage.getPageId(i).pid)
			  {
				  // set our dirId output param (dirPage already set)
				  dirId.pid = dirPage.getCurPage().pid;
				  return i;
			  }
		  }
		  // Entry was not found on this directorypage so move to the next directory page
		  nextDirectoryPageId = dirPage.getNextPage();
		  
		  if (INVALID_PAGEID != nextDirectoryPageId.pid) 
		  {
		      // unpin the current directory page, pin the next directory page
			  // for the next pass through the loop
		      Minibase.BufferManager.unpinPage(dirPage.getCurPage(), UNPIN_CLEAN);
		      Minibase.BufferManager.pinPage(nextDirectoryPageId, dirPage, PIN_DISKIO);
		  }
	  } while (INVALID_PAGEID != nextDirectoryPageId.pid);
	  
	  // If we hit this point then the entry was not found
	  return -1;

  } // protected int findEntry(PageId pageno, PageId dirId, DirPage dirPage)

  /**
   * Updates the directory entry for the given data page.
   * If the data page becomes empty, remove it.
   * If this causes a dir page to become empty, remove it
   * @param pageno identifies the data page whose directory entry will be updated
   * @param deltaRec input change in number of records on that data page
   * @param freecnt input new value of freecnt for the directory entry
   */
  protected void updateDirEntry(PageId pageno, int deltaRec, int freecnt) {
	  
	  DirPage directoryPage = new DirPage();
	  PageId directoryPageId = new PageId();
	  
	  // Find the directory page for this data page so we can update it
	  int index = findDirEntry(pageno, directoryPageId, directoryPage);
	  
	  // Update the directory page
	  directoryPage.setRecCnt(index, (short) (directoryPage.getRecCnt(index) + deltaRec));
	  directoryPage.setFreeCnt(index, (short) freecnt); 
	  
	  //Unpin the directory page to save the changes (was pinned by findDirEntry)
      Minibase.BufferManager.unpinPage(directoryPage.getCurPage(), UNPIN_DIRTY);
     
	  // If the data page is now empty, delete it
	  if (directoryPage.getRecCnt(index) < 1)
	  {
		  // deletePage will also delete the directory page if it becomes empty.
		  deletePage(pageno, directoryPageId, directoryPage, index); 
	  }
	  
  } // protected void updateEntry(PageId pageno, int deltaRec, int deltaFree)

  /**
   * Inserts a new empty data page and its directory entry into the heap file. 
   * If necessary, this also inserts a new directory page.
   * Leaves all data and directory pages unpinned
   * 
   * @return id of the new data page
   */
  protected PageId insertPage() {
	  
	  DirPage directoryPage = new DirPage();
	  PageId dataPageId = new PageId();
	  PageId nextDirectoryPageId = new PageId();
	  
	  // read the first directory page
	  Minibase.BufferManager.pinPage(headId, directoryPage, PIN_DISKIO); 

	  do 
	  {
		  if (directoryPage.getEntryCnt() < directoryPage.getMaxEntries())
		  {
			  // There is a free entry on this page, just after the last entry.
			  // Create the new data page
			  dataPageId = Minibase.DiskManager.allocate_page();
			  HFPage dataPage = new HFPage();
			  
			  // Initialize the data page's page Id
			  dataPage.setCurPage(dataPageId);
			  
			  // Add it to the directory at the appropriate slot
			  directoryPage.setPageId(directoryPage.getEntryCnt(), dataPageId);
			  
			  // Initialize free count and record count in directory slot
			  directoryPage.setFreeCnt(directoryPage.getEntryCnt(), dataPage.getFreeSpace());
			  directoryPage.setRecCnt(directoryPage.getEntryCnt(), (short) 0);			  
			  
			  // Maintain the directoryPage's entry count since we are adding a page
			  directoryPage.setEntryCnt((short)(directoryPage.getEntryCnt() + 1));
			  
			  // Copy our data page changes into the frame and pin it
			  Minibase.BufferManager.pinPage(dataPageId, dataPage, PIN_MEMCPY);
			  
			  // Unpin the directory page and the added data page
		      Minibase.BufferManager.unpinPage(directoryPage.getCurPage(), UNPIN_DIRTY);
		      Minibase.BufferManager.unpinPage(dataPage.getCurPage(), UNPIN_DIRTY);		      
			  
			  // We have inserted the page so return the pageId
			  return dataPageId;
		  }
		  // Directory page was full so move to the next directory page
		  nextDirectoryPageId = directoryPage.getNextPage();
		  
		  if (INVALID_PAGEID != nextDirectoryPageId.pid) 
		  {
		      // unpin the current directory page, pin the next directory page
			  // for the next pass through the loop
		      Minibase.BufferManager.unpinPage(directoryPage.getCurPage(), UNPIN_CLEAN);
		      Minibase.BufferManager.pinPage(nextDirectoryPageId, directoryPage, PIN_DISKIO);
	
		  }
		  else // There is no next directory page, so we must create one
		  {
			  // Create the new directory page
			  nextDirectoryPageId = Minibase.DiskManager.allocate_page();
			  
			  // Set the link to the next page in the current directory page
			  directoryPage.setNextPage(nextDirectoryPageId);
			  
			  // Save our current page id
			  PageId currentDirectoryPageId = directoryPage.getCurPage();
			  
			  //Unpin the current directory page, so changes will be saved
			  Minibase.BufferManager.unpinPage(currentDirectoryPageId, UNPIN_DIRTY);
			  
			  // Clear the current page
			  directoryPage = null;
			  directoryPage = new DirPage();
			  
			  // Save the new pages id and previous page id
			  directoryPage.setCurPage(nextDirectoryPageId);
			  directoryPage.setPrevPage(currentDirectoryPageId);
			  
			  // Pin the new directory page for the next pass through the loop, copying our changes in
			  Minibase.BufferManager.pinPage(nextDirectoryPageId, directoryPage, PIN_MEMCPY);
		  }
	  } while (INVALID_PAGEID != nextDirectoryPageId.pid);
	  
	  // Should not hit this point because the loop should ensure that we eventually
	  // insert a dataPage and return above.
	  assert(false);
	  dataPageId.pid = INVALID_PAGEID;
	  return dataPageId;
		  
  } // protected PageId insertPage()

  /**
   * Deletes the given data page and its directory entry from the heap file. If
   * appropriate, this also deletes the directory page.
   * 
   * @param pageno identifies the page to be deleted
   * @param dirId input param id of the directory page holding the data page's entry
   * @param dirPage input param to hold directory page contents
   * @param index input the data page's entry on the directory page
   */
  protected void deletePage(PageId pageno, PageId dirId, DirPage dirPage,
      int index) {

	  boolean clearDirectoryEntry = false;
	  
	  // Used for clearing page id references
	  PageId invalidPageId = new PageId(INVALID_PAGEID);
	  
	  // If this is the last page referenced by this directory, then delete
	  // this directory page
	  if (dirPage.getEntryCnt() < 2)
	  {
		  // Make sure previous and next directory pages are updated by removing the reference
		  // to this page and connecting a gap if necessary (deleting a node in a linked list)
		  
		  // Directory page is in the middle somewhere 
		  if (INVALID_PAGEID != dirPage.getPrevPage().pid && INVALID_PAGEID != dirPage.getNextPage().pid)
		  {
			  // Get the previous and next pages
			  DirPage previousPage = new DirPage();
			  DirPage nextPage = new DirPage();
			  Minibase.BufferManager.pinPage(dirPage.getPrevPage(), previousPage, PIN_DISKIO);
			  Minibase.BufferManager.pinPage(dirPage.getNextPage(), nextPage, PIN_DISKIO);
			  
			  // Set the previous next to the next, and the next previous to the previous
			  previousPage.setNextPage(nextPage.getCurPage());
			  nextPage.setPrevPage(previousPage.getCurPage());
			  
			  // Unpin (and hence save) the previous and next pages
			  Minibase.BufferManager.unpinPage(nextPage.getCurPage(), UNPIN_DIRTY);	
			  Minibase.BufferManager.unpinPage(previousPage.getCurPage(), UNPIN_DIRTY);	
			  
			  // Delete the directory page
			  Minibase.BufferManager.freePage(dirId);		  
		  }
		  // Directory page is the tail
		  else if (INVALID_PAGEID != dirPage.getPrevPage().pid)
		  {
			  // Get the previous page
			  DirPage previousPage = new DirPage();
			  Minibase.BufferManager.pinPage(dirPage.getPrevPage(), previousPage, PIN_DISKIO);
			  
			  // Clear the next page on the previous page (making it the tail)
			  previousPage.setNextPage(invalidPageId);
			  
			  // Unpin (and hence save) the previous page
			  Minibase.BufferManager.unpinPage(dirPage.getPrevPage(), UNPIN_DIRTY);
			  
			  // Delete the directory page
			  Minibase.BufferManager.freePage(dirId);		  
		  }
		  // Directory page is the head
		  else if (INVALID_PAGEID != dirPage.getNextPage().pid)
		  {			 
			  // Get the next page
			  DirPage nextPage = new DirPage();
			  Minibase.BufferManager.pinPage(dirPage.getNextPage(), nextPage, PIN_DISKIO);
			  
			  // Clear the previous page on the next page (making it the head)
			  nextPage.setPrevPage(invalidPageId);
			  
			// Unpin (and hence save) the next page
			  Minibase.BufferManager.unpinPage(dirPage.getNextPage(), UNPIN_DIRTY);
			  
			  // Delete the directory page
			  Minibase.BufferManager.freePage(dirId);
		  }
		  // In the final case where this is the last directory page 
		  // the directory page is left with the HeapFile so that it has at least one directory page.
		  else // Clear the entry for this data page
		  {
			  clearDirectoryEntry = true;
		  }
	  }
	  else // Clear the entry for this data page
	  {
		  clearDirectoryEntry = true;
	  }
	
	  if (clearDirectoryEntry) // Not necessary to clear entry when we are deleting the directory page
	  {
		  // Nullify page id, rec count, and free count
		  dirPage.setPageId(index, invalidPageId);
		  dirPage.setRecCnt(index, (short) 0);
		  dirPage.setFreeCnt(index, (short) 0);
		  // Compact the directory
		  dirPage.compact(index);
		  
		  // Pin and unpin to save the changes to the directory page
		  Minibase.BufferManager.pinPage(dirId, dirPage, PIN_MEMCPY);
		  Minibase.BufferManager.unpinPage(dirId, UNPIN_DIRTY);
	  }
	  // Delete the data page
	  Minibase.BufferManager.freePage(pageno);

  } // protected void deletePage(PageId, PageId, DirPage, int)

  /**
   * Creates an empty heapfile.  Used by HeapFile constructor.
   */
  protected void CreateEmptyHeapFile() {

	  // Add a new file entry for this heap file
	  DirPage headDirectoryPage = new DirPage();
	  headId = Minibase.DiskManager.allocate_page();
	  Minibase.DiskManager.add_file_entry(fileName, headId);
	  
	  // Initialize the head page as a directory page and save the changes
	  headDirectoryPage.setCurPage(headId);
	  Minibase.BufferManager.pinPage(headId, headDirectoryPage, PIN_MEMCPY);
	  Minibase.BufferManager.unpinPage(headId, UNPIN_DIRTY);

  } // protected void CreateEmptyHeapFile()
  
} // public class HeapFile implements GlobalConst


