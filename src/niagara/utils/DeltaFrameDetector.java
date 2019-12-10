
package niagara.utils;

import java.util.Collections;
import java.util.Iterator;
import java.util.ArrayList;

import niagara.connection_server.NiagraServer;
//import niagara.physical.PhysicalFrame;

/*
 * The FrameBuilder class handles inserting mTuples into an ArrayList,
 * sorting, and searching for completed frames. Once a frame has been found the
 * information is added to the FrameOutput subclass. This is done to make it easier
 * to update already existing frames. 
 */
public class DeltaFrameDetector
{  
  // ===========================================================
  // Constants
  // ===========================================================  
  boolean OVERRIDE_DEBUG = false;   
  private int FRAME_ID_COUNTER = 1;
  
  // ===========================================================
  // Fields
  // ===========================================================
  private String mGroupAttr = "";               // Attribute tuples are grouped by
  private String mDelta;                        // MinMax threshold
  private ArrayList<FrameTuple> mTuples;        // Array of tuples to be processed
  private ArrayList<FrameOutput> mFrames;       // Array of frames found
  private long mFrameSize;                      // Length of current frame
  //private int mStartingValue;                   // Starting value of first tuple in current frame ####################################### changed
  private double mStartingValue;                   // Starting value of first tuple in current frame
  
  //===========================================================
  // Constructors
  // ===========================================================
  public DeltaFrameDetector(FrameTuple pTuple, String pDelta)
  {
    mTuples = new ArrayList<FrameTuple>();
    mFrames = new ArrayList<FrameOutput>();
    
    mDelta = pDelta;
    mGroupAttr = pTuple.getGroupbyAttr();
    
    insertTupleIntoFrame(pTuple);
  }  
  
  // ===========================================================
  // Methods
  // ===========================================================
  public void insertTupleIntoFrame(FrameTuple pTuple)
  {    
    mTuples.add(pTuple);       // Add tuple to list
  }
    
  public ArrayList<FrameOutput> processPunctuationOnTimeAttribute(FrameTuple pTuple)
  {
    ArrayList<FrameOutput> frames = searchForFrames(pTuple.getTimeAttr());
   
    mFrames.clear();
    
    return frames;
  }
    
  private ArrayList<FrameOutput> searchForFrames(String pTime)
  {
    
    Collections.sort(mTuples);  // Sort the array to ensure tuples are in order
    ArrayList<FrameOutput> framesToOutput = new ArrayList<FrameOutput>();
    
    // Need to find index in mTuples of where to stop looking for frame
    // This is usually the time given by the punctuation
    int endIndex = indexOfFrameTuple(pTime);
    if (endIndex == -1) endIndex = indexOfClosestNextFrameTuple(pTime);
    
    // Save the starting value of the first tuple processed.
    // This is the value we check against to see if the next values are more than delta away
    
    //mStartingValue = Integer.parseInt(mTuples.get(0).getFrameAttr()); ####################################### changed
    mStartingValue = Double.parseDouble(mTuples.get(0).getFrameAttr());
    
    String startOfNextFrame = mTuples.get(0).getTimeAttr();
    String endOfNextFrame = mTuples.get(0).getTimeAttr();
    int sizeOfFrame = 1;
    int indexOfEndOfLastFrame = -1;
    
    // Loop through the rest of the tuples looking to see if any are further than mDelta away from mStartingValue
    for(int i=1; i <= endIndex; i++)
    {
      //int tupleValue = Integer.parseInt(mTuples.get(i).getFrameAttr()); ####################################### changed
      double tupleValue = Double.parseDouble(mTuples.get(i).getFrameAttr());
      
      
      // Is this tuple's value more than mDelta away from mStartingValue?
      //if (Math.abs(mStartingValue - tupleValue) >= Integer.parseInt(mDelta)) ####################################### changed
      if (Math.abs(mStartingValue - tupleValue) >= Double.parseDouble(mDelta))
      {
        // Add the frame to the output
        framesToOutput.add(new FrameOutput(startOfNextFrame,  // start
            endOfNextFrame,                     			  // end
            startOfNextFrame,                                 // frag start
            endOfNextFrame,                     			  // frag end
            "final",                                          // status
            mGroupAttr,                                       // groupby
            FRAME_ID_COUNTER++,                               // frameid
            sizeOfFrame));          						  // length
        
        indexOfEndOfLastFrame = i;
        
        if (i < endIndex)
        {
          startOfNextFrame = mTuples.get(i).getTimeAttr();          
        }
        
        mStartingValue = tupleValue;  
        sizeOfFrame = 0;
        
      }     
      
      endOfNextFrame = mTuples.get(i).getTimeAttr();
      sizeOfFrame++;
    }
    
    // Can clear state in mTuples. Remove up to index of EndOfLastFrame
    if(!framesToOutput.isEmpty())
    {
      for (int i = 0; i < indexOfEndOfLastFrame; i++)
      {
        mTuples.remove(0);
      }
    }
    
    return framesToOutput;
  }

  /**
   * 
   * @param pTime
   * @return index of frameTuple in mTuples with frameTuple.time = time
   * 
   * This needs to be faster
   */
  public int indexOfFrameTuple(String pTime)
  {
    int index = -1;

    // Loop through all stored tuples in the list to find the index of pTime.
    for(int i = 0; i < mTuples.size(); i++)
    {
      if (pTime.equalsIgnoreCase(mTuples.get(i).getTimeAttr()))
      {
        index = i;
        break;
      }
      else if (Long.parseLong(pTime) < mTuples.get(i).timeAttrAsLong())
      {
        // If 'time' is not in mTuples return -1
        break;
      }
    }
    
    return index;
  }
  
  /**
   * 
   * @param pTime
   * @return Index of tuple in mTuples with time closest to but less than pTime
   */
  public int indexOfPreviousFrameTuple(String pTime)
  {
    int index = -1;
    long timeAsLong = Long.parseLong(pTime);
    
    for(int i = 0; i < mTuples.size(); i++)
    {
      if (timeAsLong > mTuples.get(i).timeAttrAsLong())
      {
        index = i - 1;
        break;
      }
    }
    
    if (index < 0) index = 0;
    return index;
  }
  
  /**
   * 
   * @param pTime
   * @return Index of tuple in mTuples with time closest to but greater than pTime
   */
  public int indexOfClosestNextFrameTuple(String pTime)
  {
    int index = -1;
    long timeAsLong = Long.parseLong(pTime);
    
    for(int i = 0; i < mTuples.size(); i++)
    {
      if (timeAsLong < mTuples.get(i).timeAttrAsLong())
      {
        index = i;
        break;
      }
    }
    
    if(index == -1) index = mTuples.size()-1;

    return index;
  }
  
  /*public FrameOutput addFrameToFrameOutput(String pFrameStartTime, String pFrameEndTime)
  {
    return null;
    
  }
  
  public long clearState(String pTime)
  {
    // Clear state we no longer need. Can remove final mFrames and all tuples up to pTime
    
    
    
    long punctTime = Long.parseLong(pTime);

    long lastFrameEndTime = removeFramesUpTo(punctTime);
    long lastTupleRemoved;
    
    if (lastFrameEndTime != -1)
    {
      removeTuplesUpToEndOfFrame(lastFrameEndTime);
    }
    
    
    lastTupleRemoved = removeTuplesUpToPunct(punctTime);
    
    
    return lastTupleRemoved;
  }
  
  // Returns end time of last frame removed
  long removeFramesUpTo(long pTime)
  {
    long endTime = -1;
    int index = 0;
    while(index < mFrames.size())
    {
      if ((mFrames.get(index).frameStatus.equalsIgnoreCase("final")) && (mFrames.get(index).endTimeAsLong() < pTime))
      {
        endTime = mFrames.get(index).endTimeAsLong();
        mFrames.remove(index);
      }
      else if (mFrames.get(index).endTimeAsLong() > pTime)
      {
        break;
      }
      index++;
    }
    return endTime;
  }
  
  void removeTuplesUpTo(long pFrameEndTime, long pPunctTime)
  {
    int index = 0;
    while(index < mTuples.size())
    {
      // Remove tuples up to the frame we removed
      if (mTuples.get(index).timeAttrAsLong() <= pFrameEndTime)
      {
        mTuples.remove(index);
      }
      // We can remove tuples if they aren't part of a frame
      else if((mTuples.get(index).timeAttrAsLong() <= pPunctTime))
      {
        // If the predicate is true need to make sure it isn't part of a future frame
        if (mTuples.get(index).getFramePredAttr().equalsIgnoreCase("1"))
        {
          // If the tuple is more than 'mFrameSize' away from punct it can't be part of a frame
          // We'd have already detected it otherwise
          int indexOfPunct = indexOfFrameTuple(Long.toString(pPunctTime));
          if (indexOfPunct == -1)
          {
            indexOfPunct = indexOfPreviousFrameTuple(Long.toString(pPunctTime));
          }
          int indexOfFrameTuple = indexOfFrameTuple(mTuples.get(index).getTimeAttr());
          long distanceFromTupleToPunct = indexOfPunct - indexOfFrameTuple; 
          if (distanceFromTupleToPunct < mFrameSize)
          {
            return;
          }
        }
        // We can also remove all false tuples up to punctuation time
        mTuples.remove(index);
      }
      else
      {
        break;
      }
    }
  }
  
  long removeTuplesUpToPunct(long pPunctTime)
  {
    if (mTuples.isEmpty()) return pPunctTime;
    
    long lastTupleRemoved = -1;
    
    // Work backwards from index of punct to mTuples[0] removing tuples
    print("Before clear state:" + '\n' + "" + mTuples.toString());
    
    // Lookup the index in mTuples for the punctuation tuple
    int indexOfPunct = indexOfFrameTuple(Long.toString(pPunctTime));
    
    // If we are punctuating on a timestamp that isn't in data use the previous timestamp index
    if (indexOfPunct == -1)
    {
      indexOfPunct = indexOfPreviousFrameTuple(Long.toString(pPunctTime));
    }
    
    // If mTuples[punctIndex] predicate is true, tuple could be part of a future frame
    if(mTuples.get(indexOfPunct).getFramePredAttr().equalsIgnoreCase("1"))
    {
      // Can at least remove false tuples from front of index and true tuples that could
      // not possibly be part of a frame
      
      while(!mTuples.isEmpty())
      {
        if(mTuples.get(0).getFramePredAttr().equalsIgnoreCase("0"))
        {
          print("Clearing State: Removing mTuples[0] time: " + mTuples.get(0).getTimeAttr());
          lastTupleRemoved = mTuples.get(0).timeAttrAsLong();
          mTuples.remove(0);
        }
        else
        {
          // Lookup the index in mTuples for the punctuation tuple again, we might have removed tuples
          indexOfPunct = indexOfFrameTuple(Long.toString(pPunctTime));
          if (indexOfPunct == -1)
          {
            indexOfPunct = indexOfPreviousFrameTuple(Long.toString(pPunctTime));
          }
          
          // mTuples[0] predicate is true but if we are further than a mFrameSize away it cannot be part of a frame
          if (indexOfPunct > mFrameSize)
          {
            print("Clearing State: Removing mTuples[0] time: " + mTuples.get(0).getTimeAttr());
            lastTupleRemoved = mTuples.get(0).timeAttrAsLong();
            mTuples.remove(0);
          }
          else
          {
            break;
          }
        }
      }
    }
    // If mTuples[punctIndex] predicate is false, remove all tuples up to punctIndex
    else
    {   
      while(!(mTuples.isEmpty()) && 
            !(mTuples.get(0).getTimeAttr().equalsIgnoreCase(Long.toString(pPunctTime))))
      {
        print("Clearing State: Removing mTuples[0] time: " + mTuples.get(0).getTimeAttr());
        lastTupleRemoved = mTuples.get(0).timeAttrAsLong();
        mTuples.remove(0);
        indexOfPunct--;
      }
    }
    
    print("After clear state:" + '\n' + "" + mTuples.toString());
    
    if (lastTupleRemoved == -1) lastTupleRemoved = mTuples.get(0).timeAttrAsLong();
    
    return lastTupleRemoved;
  }

  void removeTuplesUpToEndOfFrame(long pFrameEndTime)
  {
    int tuplesToRemove = 0;
    
    if (mTuples.isEmpty()) return;
    
    for (int i=0; i < mTuples.size(); i++)
    {
      if (mTuples.get(i).timeAttrAsLong() <= pFrameEndTime)
      {
        tuplesToRemove++;
      }
      else
      {
        break;
      }
    }
    
    for (int i=0; i < tuplesToRemove; i++)
    {
      mTuples.remove(0);
    }
  }*/
  
  void print(String pOutput)
  {
    if (NiagraServer.DEBUG && !OVERRIDE_DEBUG)
    {
      System.out.println(pOutput);
    }
  }
  
//in case of end of stream flush out the last frame still open
  public FrameOutput flushOpenFrame(){
	  String frameStart = mTuples.get(0).getTimeAttr();
	  int size = mTuples.size();
	  String frameEnd = frameStart;
	  for(int i=1;i<size;i++){
		  frameEnd = mTuples.get(i).getTimeAttr();
	  }
      System.out.println(FRAME_ID_COUNTER+" frames. (flushOpenFrame)");
	  return new FrameOutput(
			  frameStart, 
			  frameEnd, 
			  frameStart, 
			  frameEnd, 
			  "final",
			  mGroupAttr,                                       
	          FRAME_ID_COUNTER++,                             
	          size);
  }
}
