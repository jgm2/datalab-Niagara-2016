
package niagara.utils;

import java.util.ArrayList;
import java.util.Collections;

import junit.framework.TestCase;


public class DeltaFrameDetectorTest extends TestCase
{
  private static final String TRUE = "1";
  private static final String FALSE = "0";
  
  private FrameTuple mStartTuple = new FrameTuple("1130", "1", "50");
  private DeltaFrameDetector mFrameDetector = new DeltaFrameDetector(mStartTuple, "5");
  ArrayList<FrameOutput> mFrameOut = new ArrayList<FrameOutput>();
  
  private String mGroupByAttr = "1130";
  private String mTimeAttr = "1";
  private String mFrameAttr = "10";
    
  public DeltaFrameDetectorTest(String name)
  {
    super(name);
  }
  
  public void testFrameTupleCreation()
  {
    FrameTuple ft = new FrameTuple(mGroupByAttr, mTimeAttr, mFrameAttr);
    assertEquals(ft.getGroupbyAttr(), mGroupByAttr);
    assertEquals(ft.getTimeAttr(), mTimeAttr);
    assertEquals(ft.getFrameAttr(), mFrameAttr);
  }
  
  public void testDeltaFrameDetectorFrames()
  {    
    mFrameOut = mFrameDetector.processPunctuationOnTimeAttribute(mStartTuple);
    assertEquals(mFrameOut.size(), 0);
    
    FrameTuple tuple2 = new FrameTuple("1130", "2", "48");
    FrameTuple tuple3 = new FrameTuple("1130", "3", "43");
    mFrameDetector.insertTupleIntoFrame(tuple2);
    mFrameDetector.insertTupleIntoFrame(tuple3);
    mFrameOut = mFrameDetector.processPunctuationOnTimeAttribute(tuple3);
    
    assertEquals(mFrameOut.size(), 1);
    assertEquals(mFrameOut.get(0).frameId, 1);
    assertEquals(mFrameOut.get(0).startTime, "1");
    assertEquals(mFrameOut.get(0).endTime, "3");
    
    FrameTuple tuple4 = new FrameTuple("1130", "4", "20");
    FrameTuple tuple5 = new FrameTuple("1130", "5", "22");
    FrameTuple tuple6 = new FrameTuple("1130", "6", "24");
    FrameTuple tuple7 = new FrameTuple("1130", "7", "27");
    mFrameDetector.insertTupleIntoFrame(tuple4);
    mFrameDetector.insertTupleIntoFrame(tuple5);
    mFrameDetector.insertTupleIntoFrame(tuple6);
    mFrameDetector.insertTupleIntoFrame(tuple7);
    mFrameOut = mFrameDetector.processPunctuationOnTimeAttribute(tuple7);
    
    assertEquals(mFrameOut.size(), 1);
    assertEquals(mFrameOut.get(0).frameId, 2);
    assertEquals(mFrameOut.get(0).startTime, "4");
    assertEquals(mFrameOut.get(0).endTime, "7");
  }
  
  public void testDeltaFrameDetectorMultipleFrames()
  {
    mFrameOut = mFrameDetector.processPunctuationOnTimeAttribute(mStartTuple);
    assertEquals(mFrameOut.size(), 0);
    
    FrameTuple tuple2 = new FrameTuple("1130", "2", "48");
    FrameTuple tuple3 = new FrameTuple("1130", "3", "50");    
    FrameTuple tuple4 = new FrameTuple("1130", "4", "57");
    FrameTuple tuple5 = new FrameTuple("1130", "5", "55");
    FrameTuple tuple6 = new FrameTuple("1130", "6", "59");
    FrameTuple tuple7 = new FrameTuple("1130", "7", "63");
    mFrameDetector.insertTupleIntoFrame(tuple2);
    mFrameDetector.insertTupleIntoFrame(tuple3);
    mFrameDetector.insertTupleIntoFrame(tuple4);
    mFrameDetector.insertTupleIntoFrame(tuple5);
    mFrameDetector.insertTupleIntoFrame(tuple6);
    mFrameDetector.insertTupleIntoFrame(tuple7);
    mFrameOut = mFrameDetector.processPunctuationOnTimeAttribute(tuple7);
    
    assertEquals(mFrameOut.size(), 2);
    assertEquals(mFrameOut.get(0).frameId, 1);
    assertEquals(mFrameOut.get(0).startTime, "1");
    assertEquals(mFrameOut.get(0).endTime, "4");
    assertEquals(mFrameOut.get(1).frameId, 2);
    assertEquals(mFrameOut.get(1).startTime, "4");
    assertEquals(mFrameOut.get(1).endTime, "7");
  }

  
}
