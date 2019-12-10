package niagara.physical;


import java.util.HashMap;
import java.util.Map;
import java.util.Vector;

import niagara.logical.FrameX;
import niagara.logical.Variable;
import niagara.optimizer.colombia.Cost;
import niagara.optimizer.colombia.ICatalog;
import niagara.optimizer.colombia.LogicalOp;
import niagara.optimizer.colombia.LogicalProperty;
import niagara.optimizer.colombia.Op;
import niagara.query_engine.TupleSchema;
import niagara.utils.BaseAttr;
import niagara.utils.FrameDetectorX;
import niagara.utils.FrameOutputX;
import niagara.utils.FrameTupleX;
import niagara.utils.IntegerAttr;
import niagara.utils.Punctuation;
import niagara.utils.ShutdownException;
import niagara.utils.TSAttr;
import niagara.utils.Tuple;


/**
 * This is the <code>PhysicalFrameOperator</code> that extends the
 * <code>PhysicalGroupOperator</code> with the implementation of a frame
 * 
 * @version 1.0
 * 
 */
public class PhysicalFrameX_WithoutFragments extends PhysicalOperator
{
  public String FRAME_PREDICATE_FALSE = "0";
  public String FRAME_PREDICATE_TRUE  = "1";
  
  // No blocking source streams
  private static final boolean[]          blockingSourceStreams          = { false };
  private final boolean                   OVERRIDE_DEBUG                 = true;
  
  public HashMap<Integer, FrameDetectorX> framesHashMap = new HashMap<Integer, FrameDetectorX>();
      
  private boolean inputTupleSchemaLocationsKnown = false;
  
  private String groupByAttributeName;  
  private String timeAttributeName;
  private String framePredicateAttributeName;
  
  private int frameSize;  
  private int segmentationFlag;
  
  private int groupByAttributeInputSchemaPosition;
  private int timeAttributeInputSchemaPosition;
  private int framePredicateAttributeInputSchemaPosition;
  

  private int framecount=0;
  public PhysicalFrameX_WithoutFragments()
  {
    this.setBlockingSourceStreams(PhysicalFrameX_WithoutFragments.blockingSourceStreams);
  }
  
  protected void opInitFrom(LogicalOp logicalOperator) {
    groupByAttributeName = ((FrameX) logicalOperator).getGroupByAttributeName();
    timeAttributeName = ((FrameX) logicalOperator).getTimeAttributeName();
    framePredicateAttributeName = ((FrameX) logicalOperator).getFramePredicateAttributeName();
    frameSize = ((FrameX) logicalOperator).getFrameSize();
    segmentationFlag = ((FrameX) logicalOperator).getSegmentationFlag();
  }  
  
  public void constructTupleSchema(TupleSchema[] inputSchemas)
  {

    // Frame Output Tuple Schema
    // Int:  ID
    // Long: Start Time
    // Long: End Time
    // Int:  Group By Attribute
    
    super.constructTupleSchema(inputSchemas);
   // outputTupleSchema = new TupleSchema();
    outputTupleSchema.addMapping(new Variable("frameId"));
    outputTupleSchema.addMapping(new Variable("frameStartTime"));
    outputTupleSchema.addMapping(new Variable("frameEndTime"));
    outputTupleSchema.addMapping(new Variable("frameGroupBy"));
    
    if (inputTupleSchemaLocationsKnown == false)
    {
      lookupTupleSchemaLocations();
    }     
  }
  
  @Override
  protected void processTuple(final Tuple tupleElement, final int streamId)
      throws ShutdownException
  {
    // Extract information from tuple    
    int groupByAttributeValue = Integer.parseInt(getValue(tupleElement, groupByAttributeInputSchemaPosition));
    long timeAttributeValue = Long.parseLong(getValue(tupleElement, timeAttributeInputSchemaPosition));
    boolean framePredicateAttributeValue = false;
    
    if(getValue(tupleElement, framePredicateAttributeInputSchemaPosition).equals(FRAME_PREDICATE_TRUE))
        framePredicateAttributeValue = true;
    
    // Store extracted information in a FrameTuple
    FrameTupleX fTuple = new FrameTupleX(groupByAttributeValue, timeAttributeValue, framePredicateAttributeValue);
        
    // Insert fTuple into FrameBuilder
    insertTupleIntoFrameBuilder(fTuple);
    
  }
  
  private void lookupTupleSchemaLocations()
  {
    groupByAttributeInputSchemaPosition = getAttributePosition(inputTupleSchemas[0], groupByAttributeName);    
    timeAttributeInputSchemaPosition = getAttributePosition(inputTupleSchemas[0], timeAttributeName);
    framePredicateAttributeInputSchemaPosition = getAttributePosition(inputTupleSchemas[0], framePredicateAttributeName);
    inputTupleSchemaLocationsKnown = true;
  }
    
  private String getValue(Tuple tuple, int attributeLoc)
  {
    String value;
    value = ((BaseAttr) tuple.getAttribute(attributeLoc)).attrVal().toString();
    return value;
  }
  
  private int getAttributePosition(TupleSchema schema, String attributeName)
  { 
    return schema.getPosition(attributeName);
  }
  
  private void insertTupleIntoFrameBuilder(FrameTupleX fTupleX)
  {
    // See if we already have a frame for this groupby attribute
    if (this.framesHashMap.containsKey(fTupleX.getGroupbyAttr()))
    {
      FrameDetectorX fDetectorX = this.framesHashMap.get(fTupleX.getGroupbyAttr());
      fDetectorX.insertTupleIntoFrame(fTupleX);
    }
    else
    {
      FrameDetectorX new_fBuilderX = new FrameDetectorX(fTupleX, frameSize);
      this.framesHashMap.put(fTupleX.getGroupbyAttr(), new_fBuilderX);
    }    
  }
  
  protected void processPunctuation(Punctuation inputTuple, int streamId)
  throws ShutdownException, InterruptedException 
  { 
    // Extract information from tuple    
    String groupByAttributeValue = getValue(inputTuple, groupByAttributeInputSchemaPosition);
    //int groupByAttributeValue = Integer.parseInt(getValue(inputTuple, groupByAttributeInputSchemaPosition));
    long timeAttributeValue = Long.parseLong(getValue(inputTuple, timeAttributeInputSchemaPosition));
    
    //FrameTupleX fTupleX = new FrameTupleX(groupByAttributeValue, timeAttributeValue, false);
    
    long punctuatedUpToTime = processPunctuationInFrameBuilder(groupByAttributeValue, timeAttributeValue);
    /** Time stored to punctuate up to frame end? **/
    
    // Pass on punctuation if -1!=punctuatedUpToTime
    /*if(-1!=punctuatedUpToTime){
    	outputPunctuation(streamId, punctuatedUpToTime);
    }*/
    
    outputPunctuation(streamId, punctuatedUpToTime);
  }
  
  //We can punctuate on:
  //  1: Time and GroupBy Attribute (routerid)
  //  2: Time
  //  3: GroupBy Attribute (routerid) (not implemented)
  private long processPunctuationInFrameBuilder(String pGroupBy, long pTime)
  {
    Vector<FrameOutputX> framesToOutputX = new Vector<FrameOutputX>();
    long punctuatedUpToTime = -1; 
    
    // Case 1: Time and GroupBy Attribute (routerid)
    if(!pGroupBy.equals("*"))
    {
      int groupByAsInt = Integer.parseInt(pGroupBy);
      
      if (framesHashMap.containsKey(groupByAsInt))
      {
        framesToOutputX = framesHashMap.get(groupByAsInt).processPunctuationOnTimeAttribute(pTime,segmentationFlag);
        //punctuatedUpToTime = framesHashMap.get(fTupleX.getGroupbyAttr()).clearState(fTupleX.getTimeAttr());
        framecount+=framesToOutputX.size();
      
        if(framesToOutputX != null)
        {
          // Output frames we found
          for(int i=0; i<framesToOutputX.size(); i++)
          {
            outputFrame(framesToOutputX.get(i));
            punctuatedUpToTime = framesToOutputX.get(i).endTime;
          }
          
          long tplatest = framesHashMap.get(groupByAsInt).getLatestPunctTime();
          if(tplatest>punctuatedUpToTime){
        	  punctuatedUpToTime = tplatest;
          }
          
          int size = framesToOutputX.size();
          if(0>size){
        	  System.out.println("Punct:"+punctuatedUpToTime+",Frame endtime:"+framesToOutputX.get(size-1).endTime);
          }
        }
      }
      
      
    }
    else
    {
      //currently supports only one group. if multiple groups, need to return list of punctuations for each group list!
      for (Map.Entry<Integer, FrameDetectorX> entry: framesHashMap.entrySet()) 
      {       
        framesToOutputX = entry.getValue().processPunctuationOnTimeAttribute(pTime,segmentationFlag);
        framecount+=framesToOutputX.size();
        
        if(framesToOutputX != null)
        {
          // Output frames we found
          for(int i=0; i<framesToOutputX.size(); i++)
          {
            outputFrame(framesToOutputX.get(i));
            punctuatedUpToTime = framesToOutputX.get(i).endTime;
          }
          
          long tplatest = entry.getValue().getLatestPunctTime();
          if(tplatest>punctuatedUpToTime){
        	  punctuatedUpToTime = tplatest;
          }
          
          int size = framesToOutputX.size();
          if(0<size){
        	  System.out.println(punctuatedUpToTime+","+framesToOutputX.get(size-1).endTime);
          }
        }
      }
        
    }
    
             
    return punctuatedUpToTime;
  }
  
  private void outputFrame(FrameOutputX frameOutX)
  {    
    Tuple outputTuple = new Tuple(false, 4);
    
    outputTuple.appendAttribute(new IntegerAttr(frameOutX.frameId));
    outputTuple.appendAttribute(new TSAttr(frameOutX.startTime));
    outputTuple.appendAttribute(new TSAttr(frameOutX.endTime));
    outputTuple.appendAttribute(new IntegerAttr(frameOutX.groupAttr));
            
    try
    {
      putTuple(outputTuple, 0);
    }
    catch (InterruptedException e)
    {
      e.printStackTrace();
    }
    catch (ShutdownException e)
    {
      e.printStackTrace();
    }

  }
  
  private void outputPunctuation(int streamId, long time)
  {
    Punctuation punctuationTuple = new Punctuation(false, 4);
        
    punctuationTuple.appendAttribute(BaseAttr.createWildStar(BaseAttr.Type.Int));
    punctuationTuple.appendAttribute(new TSAttr(Long.toString(time)));
    punctuationTuple.appendAttribute(BaseAttr.createWildStar(BaseAttr.Type.TS));
    //for fragment output in case of long frames
    /*punctuationTuple.appendAttribute(BaseAttr.createWildStar(BaseAttr.Type.TS));
    punctuationTuple.appendAttribute(BaseAttr.createWildStar(BaseAttr.Type.TS));
    punctuationTuple.appendAttribute(BaseAttr.createWildStar(BaseAttr.Type.String));
    punctuationTuple.appendAttribute(BaseAttr.createWildStar(BaseAttr.Type.Int));*/
    punctuationTuple.appendAttribute(BaseAttr.createWildStar(BaseAttr.Type.Int));
        
    try
    {
      putTuple(punctuationTuple, 0);
    }
    catch (InterruptedException e)
    {
      e.printStackTrace();
    }
    catch (ShutdownException e)
    {
      e.printStackTrace();
    }
  }
  
  void print(String output)
  {
    System.out.println(output);
  }

  @Override
  public boolean isStateful()
  {
    return true;
  }

  /**
   * vpapad's code, jmw no changes
   */
  public Cost findLocalCost(final ICatalog catalog,
      final LogicalProperty[] InputLogProp)
  {
    // XXX vpapad: really naive. Only considers the hashing cost
    final float inpCard = InputLogProp[0].getCardinality();
    final float outputCard = this.logProp.getCardinality();

    double cost = inpCard * catalog.getDouble("tuple_reading_cost");
    cost += inpCard * catalog.getDouble("tuple_hashing_cost");
    cost += outputCard * catalog.getDouble("tuple_construction_cost");
    return new Cost(cost);
  }
  
  @Override
  public Op opCopy()
  {
    final PhysicalFrameX_WithoutFragments frame = new PhysicalFrameX_WithoutFragments();
    frame.groupByAttributeName = this.groupByAttributeName;
    frame.timeAttributeName = this.timeAttributeName;
    frame.framePredicateAttributeName = this.framePredicateAttributeName;
    frame.frameSize = this.frameSize;
    frame.segmentationFlag = this.segmentationFlag;
    return frame;
  }

  @Override
  public boolean equals(final Object other)
  {
    if ((other == null) || !(other instanceof PhysicalFrameX_WithoutFragments))
    {
      return false;
    }
    if (other.getClass() != PhysicalFrameX_WithoutFragments.class)
    {
      return other.equals(this);
    }
    final PhysicalFrameX_WithoutFragments f = (PhysicalFrameX_WithoutFragments) other;
    return (this.framePredicateAttributeName.equals(f.framePredicateAttributeName))
        && (this.frameSize == f.frameSize) && (this.groupByAttributeName
        == f.groupByAttributeName) && (this.segmentationFlag == f.segmentationFlag);
  }


  @Override
  public int hashCode()
  {
    return this.framesHashMap.hashCode();    
  }
  
  //when stream is closed, then flush any frames still open in memory
  public void streamClosed(int streamId) throws ShutdownException {
	  FrameOutputX framesToOutputX = new FrameOutputX();
	  long punctuatedUpToTime = -1;
	  for (Map.Entry<Integer, FrameDetectorX> entry: framesHashMap.entrySet()) 
      {       
        framesToOutputX = entry.getValue().flushOpenFrame(segmentationFlag);
        
        if(null != framesToOutputX)
        {
            outputFrame(framesToOutputX);
            punctuatedUpToTime = framesToOutputX.endTime;
            // Pass on punctuation.
        	outputPunctuation(streamId, punctuatedUpToTime);
          } else 
        	  framecount++;
        }
	
	//System.out.println(framecount+" frames. (frame fragements count)");
      }
	
  
}
