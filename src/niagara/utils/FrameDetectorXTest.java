
package niagara.utils;

import java.util.Vector;
import java.util.Collections;

import junit.framework.TestCase;


public class FrameDetectorXTest //extends TestCase
{

  /*private int x = 1;
  private int y = 1;

  // Router example
  // groupbyattr = routerid
  // frameattr   = packetloss
  // timeattr    = time
  // predicate   = high/low packetloss, 1=true 0=false
  private int groupByAttr = 1130;
  private long timeAttr = 1;
  private boolean framePredicate = false;
  private boolean TRUE = true;
  private boolean FALSE = false;
  
  private FrameDetectorX frameDetectorX = new FrameDetectorX();
  Vector<FrameOutputX> frameOut = new Vector<FrameOutputX>();
  
  *//**
   * @param name
   *//*
  public FrameDetectorXTest(String name)
  {
    super(name);
  }

  public void testAddition()
  {
    int z = x + y;
    assertEquals(z, 2);
  }
  
  public void testFrameTupleCreation()
  {
    FrameTupleX ft = new FrameTupleX(groupByAttr, timeAttr, framePredicate);
    assertEquals(ft.getGroupbyAttr(), groupByAttr);
    assertEquals(ft.getTimeAttr(), timeAttr);    
    assertEquals(ft.getFramePredAttr(), framePredicate);
  }
  
  public void testFrameTupleInsert()
  {    
    FrameTupleX ft = new FrameTupleX(groupByAttr, timeAttr, TRUE);    
    frameDetectorX.insertTupleIntoFrame(ft);
    assertEquals(frameDetectorX.frameTuples.get(0), ft);       
  }
  
  public void testFrameOrdering()
  {    
    FrameTupleX ft1 = new FrameTupleX(groupByAttr, 2, TRUE);
    frameDetectorX.insertTupleIntoFrame(ft1);
    
    FrameTupleX ft2 = new FrameTupleX(groupByAttr, 1, TRUE);
    frameDetectorX.insertTupleIntoFrame(ft2);
    
    assertEquals(frameDetectorX.frameTuples.get(0), ft1);
    assertEquals(frameDetectorX.frameTuples.get(1), ft2);
    
    Collections.sort(frameDetectorX.frameTuples);
    assertEquals(frameDetectorX.frameTuples.get(0), ft2);
    assertEquals(frameDetectorX.frameTuples.get(1), ft1);
  }
  
  public Vector<FrameTupleX> setupFrameTuples(String sequence)
  {
    Vector<FrameTupleX> frameTuples = new Vector<FrameTupleX>();
    
    
    for(int i=0; i<sequence.length(); i++)
    {
      boolean predicate = false;
      if(sequence.charAt(i) == 't' || sequence.charAt(i) == 'T')
      {
        predicate = true;
      }
   
      frameTuples.add(new FrameTupleX(groupByAttr, i+1, predicate));      
    }
    
    return frameTuples;
  }
  
  public void testSetupFrameTuples()
  {
    Vector<FrameTupleX> frameTuples = new Vector<FrameTupleX>();
    
    frameTuples = setupFrameTuples("F");
    assertEquals(1, frameTuples.get(0).getTimeAttr());
    assertEquals(false, frameTuples.get(0).getFramePredAttr());
    
    frameTuples = setupFrameTuples("T");
    assertEquals(1, frameTuples.get(0).getTimeAttr());
    assertEquals(true, frameTuples.get(0).getFramePredAttr());
    
    frameTuples = setupFrameTuples("FTF");
    assertEquals(1, frameTuples.get(0).getTimeAttr());
    assertEquals(false, frameTuples.get(0).getFramePredAttr());
    assertEquals(2, frameTuples.get(1).getTimeAttr());
    assertEquals(true, frameTuples.get(1).getFramePredAttr());
    assertEquals(3, frameTuples.get(2).getTimeAttr());
    assertEquals(false, frameTuples.get(2).getFramePredAttr());
    
    frameTuples = setupFrameTuples("TTFTF");
    assertEquals(true, frameTuples.get(0).getFramePredAttr());
    assertEquals(true, frameTuples.get(1).getFramePredAttr());
    assertEquals(false, frameTuples.get(2).getFramePredAttr());    
    assertEquals(true, frameTuples.get(3).getFramePredAttr());    
    assertEquals(false, frameTuples.get(4).getFramePredAttr());
  }
  
  // TTTFpFp
  public void testSearchForFramesTTTFp()
  {
    frameDetectorX.frameSize = 3;
    
    FrameTupleX ft1 = new FrameTupleX(groupByAttr, 1, TRUE);
    frameDetectorX.insertTupleIntoFrame(ft1);
    FrameTupleX ft2 = new FrameTupleX(groupByAttr, 2, TRUE);
    frameDetectorX.insertTupleIntoFrame(ft2);   
    FrameTupleX ft3 = new FrameTupleX(groupByAttr, 3, TRUE);
    frameDetectorX.insertTupleIntoFrame(ft3);
        
    FrameTupleX ft4 = new FrameTupleX(groupByAttr, 4, FALSE);
    frameDetectorX.insertTupleIntoFrame(ft4);    
    
//    frameOut = frameDetectorX.searchForFrames(4);
//    assertEquals(1, frameOut.size());
//    assertEquals(1, frameOut.get(0).startTime);
//    assertEquals(3, frameOut.get(0).endTime);

    
    FrameTupleX ft4p = new FrameTupleX(groupByAttr, 4, FALSE);
    frameOut = frameDetectorX.processPunctuationOnTimeAttribute(ft4p.getTimeAttr());    
    assertEquals(1, frameOut.size());
    assertEquals(1, frameOut.get(0).startTime);
    assertEquals(3, frameOut.get(0).endTime);
    
    FrameTupleX ft5 = new FrameTupleX(groupByAttr, 5, FALSE);
    frameDetectorX.insertTupleIntoFrame(ft5);    
        
    FrameTupleX ft5p = new FrameTupleX(groupByAttr, 5, FALSE);
    frameOut = frameDetectorX.processPunctuationOnTimeAttribute(ft5p.getTimeAttr());    
    assertEquals(0, frameOut.size());
  }
  
  public void testTTTFp()
  {
    frameDetectorX.frameSize = 3;
    
    Vector<FrameTupleX> frameTuples = setupFrameTuples("TTTF");
    
    for(int i=0; i<frameTuples.size(); i++)
    {
      frameDetectorX.insertTupleIntoFrame(frameTuples.get(i));
    }
        
    frameOut = frameDetectorX.processPunctuationOnTimeAttribute(4);    
    assertEquals(1, frameOut.size());
    assertEquals(1, frameOut.get(0).startTime);
    assertEquals(3, frameOut.get(0).endTime);
    
    FrameTupleX ft5 = new FrameTupleX(groupByAttr, 5, FALSE);
    frameDetectorX.insertTupleIntoFrame(ft5);    
        
    FrameTupleX ft5p = new FrameTupleX(groupByAttr, 5, FALSE);
    frameOut = frameDetectorX.processPunctuationOnTimeAttribute(ft5p.getTimeAttr());    
    assertEquals(0, frameOut.size());
  }
  
  public void testFFTTTTTTTF()
  {
    frameDetectorX.frameSize = 5;
    
    Vector<FrameTupleX> frameTuples = setupFrameTuples("FFTTTTTTTF");
    
    for(int i=0; i<frameTuples.size(); i++)
    {
      frameDetectorX.insertTupleIntoFrame(frameTuples.get(i));
    }
        
    frameOut = frameDetectorX.processPunctuationOnTimeAttribute(10);    
    assertEquals(1, frameOut.size());
    assertEquals(3, frameOut.get(0).startTime);
    assertEquals(9, frameOut.get(0).endTime);
  }
  
  public void testFTpTTTTTTpFpT()
  {
    frameDetectorX.frameSize = 5;
    
    Vector<FrameTupleX> frameTuples = setupFrameTuples("FTTTTTTTFT");
    
    for(int i=0; i<frameTuples.size(); i++)
    {
      frameDetectorX.insertTupleIntoFrame(frameTuples.get(i));
    }
        
    frameOut = frameDetectorX.processPunctuationOnTimeAttribute(2); 
    assertEquals(0, frameOut.size());
    frameOut = frameDetectorX.processPunctuationOnTimeAttribute(8);
    assertEquals(0, frameOut.size());
    frameOut = frameDetectorX.processPunctuationOnTimeAttribute(9);
    assertEquals(1, frameOut.size());
    assertEquals(2, frameOut.get(0).startTime);
    assertEquals(8, frameOut.get(0).endTime);
  }*/

}
