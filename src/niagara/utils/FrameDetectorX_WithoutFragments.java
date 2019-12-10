
package niagara.utils;

import java.util.Collections;
import java.util.Vector;
import niagara.connection_server.NiagraServer;

/*
 * The FrameDetectorX
 */
public class FrameDetectorX_WithoutFragments
{ 
  /*public String FRAME_PREDICATE_FALSE = "0";
  public String FRAME_PREDICATE_TRUE  = "1";*/
  boolean OVERRIDE_DEBUG = false;
  
  private int FRAME_ID_COUNTER = 1;
  private int groupAttr;
  private int frameSize;
  
  private long currentFrameStartTime = -1;
  private long currentFrameEndTime = -1;
  private int currentFrameCount = 0;
  private boolean currentFrameClosed = false;
  private static int numTuplesInAFrame = 0;
  
  private int belowThresholdFrameCount = 0;
  private int aboveThresholdFrameCount = 0;
  
  private int avgFrameSize = 0;
  
  private Vector<FrameTupleX> frameTuples;
  //public Vector<FrameOutputX> frames;
  
//  public long earliestStartTime = -1;
//  public long latestEndTime = -1;  
  
  private long latestPunctTime=-1;
  
  public long getLatestPunctTime() {
	return latestPunctTime;
}

public FrameDetectorX_WithoutFragments()
  {
    frameTuples = new Vector<FrameTupleX>();
    //frames = new Vector<FrameOutputX>();
  }
  
  public FrameDetectorX_WithoutFragments(FrameTupleX fTupleX, int frameSize)
  {
    groupAttr = fTupleX.getGroupbyAttr();
    frameTuples = new Vector<FrameTupleX>();
    //frames = new Vector<FrameOutputX>();
    this.frameSize = frameSize;
        
    frameTuples.add(fTupleX);    
  }
  
  public void insertTupleIntoFrame(FrameTupleX fTupleX)
  {    
    // If the list is empty and this tuple is false don't even add it to the list
//    if (frameTuples.isEmpty() && (fTupleX.getFramePredAttr() == false))
//      return;
//    else
      frameTuples.add(fTupleX);       // Add tuple to Vector list
  }
  
//  public int numberOfTruePredicateTuplesToRight(int index)
//  {
//    int tuplesToRight = 0;
//    
//    for (int i = index + 1; i < frameTuples.size(); i++)
//    {
//      FrameTupleX currentTuple = frameTuples.get(i);
//
//      if (currentTuple.getFramePredAttr() == true)
//      {
//        tuplesToRight++;
//      }
//      else
//      {
//        break;
//      }
//    }
//    
//    return tuplesToRight;
//  }
  
  
  public Vector<FrameOutputX> processFramesNoSegmentation(long pPuncuationTime){
	  
	  Vector<FrameOutputX> framesToOutput = new Vector<FrameOutputX>();
	    FrameTupleX tuple;
	    
	    latestPunctTime = -1;
	    
	    //boolean longFramehandle = true;
	    
	    // Process frameTuples up to punctuation
	    while(frameTuples.firstElement().getTimeAttr() <= pPuncuationTime)
	    {
	      // Remove first element
	      tuple = frameTuples.remove(0);
	      
	     //frame only on tuples that satisfy framepredicate
		      
	      // If tuple predicate is true
	      if (tuple.getFramePredAttr() == true)
	      {
	        // If this is first true, save start time in case it's a start of a frame
	        if (currentFrameCount == 0)
	        {
	          currentFrameStartTime = tuple.getTimeAttr();
	          currentFrameClosed = false;
	        }
	        
	        // increment current count of how many true tuples we've seen in a row
	        currentFrameCount++;
	        
	        // save current end time in case this is the last tuple in a frame
	        currentFrameEndTime = tuple.getTimeAttr();
	        
	        //update the frame start time to first tuple in new fragment 
	        /*if(longFramehandle && !currentFrameClosed){
	        	currentFrameStartTime = currentFrameEndTime;
	        	longFramehandle = false;
	        } else if(longFramehandle && currentFrameClosed){
	        	longFramehandle = false;
	        }*/
	      }
	      // If tuple predicate is false
	      else
	      {
	        // See if we just closed a frame
	        if (currentFrameCount >= frameSize)
	        { 
	          //if last frame fragment was output before punctuation, only increment frame id
	          //if(!longFramehandle){
	        	  //System.out.println(FRAME_ID_COUNTER+","+currentFrameStartTime+","+currentFrameEndTime+","+currentFrameCount+","+"0");
	        	  avgFrameSize += currentFrameCount;
	        	  framesToOutput.add(new FrameOutputX(currentFrameStartTime, currentFrameEndTime, groupAttr, FRAME_ID_COUNTER++));
	          /*} else {
	          FRAME_ID_COUNTER++;
	          }*/
	          
	          //longFramehandle = false;       
	          numTuplesInAFrame += currentFrameCount;          
	          //System.out.println("Total number of tuples contributing to a frame: " + numTuplesInAFrame);
	          
	        }
	        currentFrameStartTime = -1;
	        currentFrameClosed = true;
	        // Reset frame count to 0
	        currentFrameCount = 0;
	        
	      }
	      
	      
	      if(frameTuples.isEmpty()) break;
	    }//end of while
	    
	    //output frame fragment if last frame is open
	    /*if(!currentFrameClosed){
    		if (currentFrameCount >= frameSize){
    			//System.out.println(FRAME_ID_COUNTER+","+currentFrameStartTime+","+currentFrameEndTime+","+currentFrameCount+","+"1");
    			framesToOutput.add(new FrameOutputX(currentFrameStartTime, currentFrameEndTime, groupAttr, FRAME_ID_COUNTER));
		    	currentFrameStartTime = currentFrameEndTime;
		    	latestPunctTime = currentFrameEndTime;
    		} else {
    			latestPunctTime = currentFrameStartTime;
    		}
	    	
	    } //if last tuple was below threshold and not part of frame
    	else*/ 
	    if(currentFrameClosed){
    			latestPunctTime = pPuncuationTime;
	    } 
	    
	    //could choose to output punctuation but danger of repeating punctuation for long frames at each input punctuation
	    /*else {
	    	latestPunctTime = currentFrameStartTime;
	    }*/
	    
	    // Check counter for partial frame
	    if (currentFrameCount >= frameSize)
	    {
	      // partial frame detected
	    }
	     
	    
	    return framesToOutput;
  }
  
  public Vector<FrameOutputX> processFramesWithSegmentation(long pPuncuationTime){
	  
	Vector<FrameOutputX> framesToOutput = new Vector<FrameOutputX>();
	    FrameTupleX tuple;
	    
	    latestPunctTime = -1;
    //boolean longFramehandle = true;
    
    // Process frameTuples up to punctuation
    while(frameTuples.firstElement().getTimeAttr() <= pPuncuationTime)
    {
      // Remove first element
      tuple = frameTuples.remove(0);
	      
	    //if segmentation flag is set, we have frames on all tuples either as satisfying framepredicate or not
	    	
    	// If tuple predicate is true
	    if (tuple.getFramePredAttr() == true)
	    {
	    	// See if we just closed a frame
	        if (belowThresholdFrameCount >= frameSize)
	        {
	          //if(!longFramehandle){
	        	  //System.out.println(FRAME_ID_COUNTER+","+currentFrameStartTime+","+currentFrameEndTime+","+currentFrameCount+","+"0");
	        	  framesToOutput.add(new FrameOutputX(currentFrameStartTime, currentFrameEndTime, groupAttr, FRAME_ID_COUNTER++));
	          /*} else {
		          FRAME_ID_COUNTER++;
		          }*/
	          currentFrameClosed = true;
	          //longFramehandle = false;
	          numTuplesInAFrame += belowThresholdFrameCount;          
	          //System.out.println("Total number of tuples contributing to a frame: " + numTuplesInAFrame);
	        }
	          
	          // Reset frame count to 0
	          belowThresholdFrameCount = 0;
	          
	        
	    	// If this is first true, save start time in case it's a start of a frame
	        if (aboveThresholdFrameCount == 0)
	        {
	          currentFrameStartTime = tuple.getTimeAttr();
	          currentFrameClosed = false;
	        }
	        
	        // increment current count of how many true tuples we've seen in a row
	        aboveThresholdFrameCount++;
	        
	        // save current end time in case this is the last tuple in a frame
	        currentFrameEndTime = tuple.getTimeAttr();
	        
	        /*if(longFramehandle && !currentFrameClosed){
	        	currentFrameStartTime = currentFrameEndTime;
	        	longFramehandle = false;
	        }*/
	        
	    } // If tuple predicate is false
	      else
	      {
	        // See if we just closed a frame
	        if (aboveThresholdFrameCount >= frameSize)
	        {
	        	//if(!longFramehandle){
	        		//System.out.println(FRAME_ID_COUNTER+","+currentFrameStartTime+","+currentFrameEndTime+","+currentFrameCount+","+"0");
	        	  framesToOutput.add(new FrameOutputX(currentFrameStartTime, currentFrameEndTime, groupAttr, FRAME_ID_COUNTER++));
	        	/*} else {
	  	          FRAME_ID_COUNTER++;
		          }*/
	        	
	        	currentFrameClosed = true;
	        	//longFramehandle = false;
	        	numTuplesInAFrame += aboveThresholdFrameCount;          
		        //System.out.println("Total number of tuples contributing to a frame: " + numTuplesInAFrame);
	        }
	        
	        // Reset frame count to 0
	        aboveThresholdFrameCount = 0;
	        
	        // If this is first false, save start time in case it's a start of a frame
	        if (belowThresholdFrameCount == 0)
	        {
	          currentFrameStartTime = tuple.getTimeAttr();
	          currentFrameClosed = false;
	        }
	        
	        // increment current count of how many false tuples we've seen in a row
	        belowThresholdFrameCount++;
	        
	        // save current end time in case this is the last tuple in a frame
	        currentFrameEndTime = tuple.getTimeAttr();
	        
	        /*if(longFramehandle && !currentFrameClosed){
	        	currentFrameStartTime = currentFrameEndTime;
	        	longFramehandle = false;
	        }*/
	        
	      }
	      
	      if(frameTuples.isEmpty()) break;
	    }//end of while
	    
	    /*//output frame fragment if last frame is open
	    if(!currentFrameClosed){
	    	  	if(belowThresholdFrameCount >= frameSize || aboveThresholdFrameCount >= frameSize){
		    		//System.out.println(FRAME_ID_COUNTER+","+currentFrameStartTime+","+currentFrameEndTime+","+currentFrameCount+","+"1");
			    	framesToOutput.add(new FrameOutputX(currentFrameStartTime, currentFrameEndTime, groupAttr, FRAME_ID_COUNTER));
			    	currentFrameStartTime = currentFrameEndTime;
			    	latestPunctTime = currentFrameEndTime;
		    	} else {
	    			latestPunctTime = currentFrameStartTime;
	    		}
	    }*/
	    if(currentFrameClosed){
			latestPunctTime = pPuncuationTime;
	    } 
    
    		    
	    // Check counter for partial frame
	    if (currentFrameCount >= frameSize)
	    {
	      // partial frame detected
	    }
	     
	    
	    return framesToOutput;
  }
  
  public Vector<FrameOutputX> processPunctuationOnTimeAttribute(long pPuncuationTime, int segmentationFlag)
  {
    if(frameTuples.isEmpty()) return new Vector<FrameOutputX>() ;
    
    Collections.sort(frameTuples);
    
    Vector<FrameOutputX> framesToOutput = new Vector<FrameOutputX>();
    
    if(0==segmentationFlag){
    	framesToOutput = processFramesNoSegmentation(pPuncuationTime);
    } else {
    	framesToOutput = processFramesWithSegmentation(pPuncuationTime);
    }
    
    return framesToOutput;
  }
    
  void print(String output)
  {
    if (NiagraServer.DEBUG && !OVERRIDE_DEBUG)
    {
      System.out.println(output);
    }
  }
  
  //in case of end of stream flush out the last frame still open
  public FrameOutputX flushOpenFrame(int segmentationFlag){
	  FrameOutputX frameOutputX = null;
	  if(!currentFrameClosed) {
		 if(1==segmentationFlag){
			 numTuplesInAFrame += aboveThresholdFrameCount+belowThresholdFrameCount;
		 } else {
			 numTuplesInAFrame += currentFrameCount;
		 }
		          
         //System.out.println("Total number of tuples contributing to a frame: " + numTuplesInAFrame);
		 if(currentFrameStartTime != currentFrameEndTime){
			 //System.out.println(FRAME_ID_COUNTER+","+currentFrameStartTime+","+currentFrameEndTime+","+currentFrameCount+","+"0");
			 frameOutputX = new FrameOutputX(currentFrameStartTime, currentFrameEndTime, groupAttr, FRAME_ID_COUNTER++);
			 avgFrameSize += currentFrameCount;
		 }
	  }
	  //System.out.println(FRAME_ID_COUNTER+"-1 frames (whole frame count)");
	  //System.out.println(avgFrameSize+" "+avgFrameSize/(FRAME_ID_COUNTER-1));
	  return frameOutputX;
	    
	    
  }
}
