package niagara.physical;

import java.util.ArrayList;
import java.util.Enumeration;
import java.util.Hashtable;
import java.util.Map;

import niagara.optimizer.colombia.Cost;
import niagara.optimizer.colombia.ICatalog;
import niagara.optimizer.colombia.LogicalOp;
import niagara.optimizer.colombia.LogicalProperty;
import niagara.optimizer.colombia.Op;
import niagara.optimizer.colombia.PhysicalProperty;
import niagara.query_engine.TupleSchema;
import niagara.utils.BaseAttr;
import niagara.utils.ControlFlag;
//import niagara.utils.FrameDetector;
import niagara.utils.FrameDetectorX;
import niagara.utils.FrameOutput;
import niagara.utils.FrameOutputX;
import niagara.utils.FrameTuple;
import niagara.utils.IntegerAttr;
import niagara.utils.Punctuation;
import niagara.utils.ShutdownException;
import niagara.utils.StringAttr;
import niagara.utils.DeltaFrameDetector;
import niagara.utils.TSAttr;
import niagara.utils.Tuple;

import niagara.connection_server.NiagraServer;
import niagara.logical.DeltaFrame;
import niagara.logical.Variable;

/**
 * Physical implementation of PhysicalDeltaFrame operator.
 * Punctuation on frameStart.
 * Uses src/util/DeltaFrameDetector.java
 * 
 */
public class PhysicalDeltaFrame extends PhysicalOperator
{
  // ===========================================================
  // Constants
  // =========================================================== 
  private static final boolean[]                   blockingSourceStreams           = { false };
  private final boolean                            OVERRIDE_DEBUG                  = true;

  // ===========================================================
  // Fields
  // ===========================================================
  private String                                   mGroupByAttribute;
  private String                                   mAggregateAttribute;
  private String                                   mTime;
  private String                                   mDelta;
  
  private boolean                                  mInputTupleSchemaLocationsKnown = false;
  private int                                      mGroupByAttributeInputSchemaPosition;
  private int                                      mAggregateAttributeInputSchemaPosition;
  private int                                      mTimeInputSchemaPosition;
  
  private int framecount=0;
  /**
   * Hashtable keyed on group by attributes. In traffic example, framing on
   * detectors 1130, and 1131, each would be a key into this hashtable with
   * their own frameDetector.
   */
  public Hashtable<String, DeltaFrameDetector> mDetectorsHashtable                 = new Hashtable<String, DeltaFrameDetector>();

  // ===========================================================
  // Constructors
  // ===========================================================
  public PhysicalDeltaFrame()
  {
    this.setBlockingSourceStreams(PhysicalDeltaFrame.blockingSourceStreams);
  }

  //===========================================================
  // Methods for/from SuperClass/Interfaces
  // ===========================================================
  @Override
  protected void opInitialize()
  {
  }
  
  @Override
  /**
   * Load query plan parameters from the Logical operator
   */
  protected void opInitFrom(final LogicalOp pLogicalOperator)
  {
    this.mGroupByAttribute = ((DeltaFrame) pLogicalOperator).getGroupByAttribute();
    this.mAggregateAttribute = ((DeltaFrame) pLogicalOperator).getAggregateAttribute();
    this.mTime = ((DeltaFrame) pLogicalOperator).getTimeAttribute();
    this.mDelta = ((DeltaFrame) pLogicalOperator).getTarget();
  }
  
  @Override
  public boolean isStateful()
  {
    return true;
  }
  
  @Override
  /**
   * vpapad's code, jmw no changes
   */
  public Cost findLocalCost(final ICatalog pCatalog, final LogicalProperty[] pInputLogProp)
  {
    // XXX vpapad: really naive. Only considers the hashing cost
    final float inpCard = pInputLogProp[0].getCardinality();
    final float outputCard = this.logProp.getCardinality();

    double cost = inpCard * pCatalog.getDouble("tuple_reading_cost");
    cost += inpCard * pCatalog.getDouble("tuple_hashing_cost");
    cost += outputCard * pCatalog.getDouble("tuple_construction_cost");
    return new Cost(cost);
  }

  @Override
  public Op opCopy()
  {
    final PhysicalDeltaFrame frame = new PhysicalDeltaFrame();
    frame.mGroupByAttribute = this.mGroupByAttribute;
    frame.mAggregateAttribute = this.mAggregateAttribute;
    frame.mTime = this.mTime;
    frame.mDelta = this.mDelta;
    return frame;
  }

  @Override
  public boolean equals(final Object pOther)
  {
    if ((pOther == null) || !(pOther instanceof PhysicalDeltaFrame))
    {
      return false;
    }
    if (pOther.getClass() != PhysicalDeltaFrame.class)
    {
      return pOther.equals(this);
    }

    final PhysicalDeltaFrame aggFrame = (PhysicalDeltaFrame) pOther;
    return ((this.mAggregateAttribute.equals(aggFrame.mAggregateAttribute)) && (this.mGroupByAttribute.equals(aggFrame.mGroupByAttribute)) && (this.mDelta.equals(aggFrame.mDelta)) && (this.mTime.equals(aggFrame.mTime)));
  }

  @Override
  public int hashCode()
  {
    // Combining groupby and frameattribute strings and hashing over that
    // should be good enough? not really sure when this gets used
    final String hashString = this.mGroupByAttribute + this.mAggregateAttribute;
    return hashString.hashCode();
  }

  /**
   * @see niagara.optimizer.colombia.PhysicalOp#FindPhysProp(PhysicalProperty[])
   */
  @Override
  public PhysicalProperty findPhysProp(final PhysicalProperty[] input_phys_props)
  {
    return input_phys_props[0];
  }
  
  @Override
  void processCtrlMsgFromSink(final ArrayList pCtrl, final int pStreamId) throws java.lang.InterruptedException, ShutdownException
  {
    // downstream control message is GET_PARTIAL
    // We should not get SYNCH_PARTIAL, END_PARTIAL, EOS or NULLFLAG
    // REQ_BUF_FLUSH is handled inside SinkTupleStream
    // here (SHUTDOWN is handled with exceptions)
    // jmw: not my code

    if (pCtrl == null)
    {
      return;
    }

    final ControlFlag ctrlFlag = (ControlFlag) pCtrl.get(0);

    switch (ctrlFlag)
    {
      default:
        assert false : "JMW unexpected control message from sink " + ctrlFlag.flagName();
    }
  }
  
  @Override
  /**
   * construct output tuple schema
   */
  public void constructTupleSchema(final TupleSchema[] pInputSchemas)
  {

    // Frame Output Tuple Schema
    // Int: Frame ID
    // TS: Frame Start Time
    // TS: Frame End Time
    // TS: Frame Fragment Start Time
    // TS: Frame Fragment End Time
    // String: Frame Status (partial/final)
    // Int: Frame Group By Attribute (ie, detectorid)
    // Int: Frame Length

    super.constructTupleSchema(pInputSchemas);
    this.outputTupleSchema = new TupleSchema();
    this.outputTupleSchema.addMapping(new Variable("frameId"));
    this.outputTupleSchema.addMapping(new Variable("frameStartTime"));
    this.outputTupleSchema.addMapping(new Variable("frameEndTime"));
    this.outputTupleSchema.addMapping(new Variable("frameFragmentStartTime"));
    this.outputTupleSchema.addMapping(new Variable("frameFragmentEndTime"));
    this.outputTupleSchema.addMapping(new Variable("frameStatus"));
    this.outputTupleSchema.addMapping(new Variable("frameGroupBy"));
    this.outputTupleSchema.addMapping(new Variable("frameLength"));

    if (this.mInputTupleSchemaLocationsKnown == false)
    {
      this.lookupTupleSchemaLocations();
    }
  }
  
  @Override
  /**
   * This gets called for each tuple passing through the system
   * 
   */
  protected void processTuple(final Tuple pTupleElement, final int pStreamId) throws ShutdownException
  {
    // Store the tuples information in a FrameTuple
    final FrameTuple fTuple = new FrameTuple(
        this.getValue(pTupleElement, this.mGroupByAttributeInputSchemaPosition), 
        this.getValue(pTupleElement, this.mTimeInputSchemaPosition), 
        this.getValue(pTupleElement, this.mAggregateAttributeInputSchemaPosition));

    // Insert fTuple into Frame Detector
    this.insertTupleIntoAggregateFrameDetector(fTuple);

    return;
  }

  @Override
  protected void processPunctuation(final Punctuation pInputTuple, final int pStreamId) throws ShutdownException, InterruptedException
  {

    // Extract information from tuple
    final FrameTuple fTuple = new FrameTuple(this.getValue(pInputTuple, this.mGroupByAttributeInputSchemaPosition), this.getValue(pInputTuple, this.mTimeInputSchemaPosition), this.getValue(pInputTuple, this.mAggregateAttributeInputSchemaPosition), "*");

    final long punctuatedUpToTime = this.processPunctuationInAggregateFrameDetector(fTuple);

    // Pass on punctuation.
    this.outputPunctuation(punctuatedUpToTime);
  }

  // ===========================================================
  // Methods
  // ===========================================================
  /**
   * Lookup the indexes within the tuple schema of our parameters.
   * This is so we don't have to search for the attribute position in the schema
   * each time we read a tuple value
   */
  private void lookupTupleSchemaLocations()
  {
    this.mGroupByAttributeInputSchemaPosition = this.getAttributePosition(this.inputTupleSchemas[0], this.mGroupByAttribute);
    this.mAggregateAttributeInputSchemaPosition = this.getAttributePosition(this.inputTupleSchemas[0], this.mAggregateAttribute);    
    this.mTimeInputSchemaPosition = this.getAttributePosition(this.inputTupleSchemas[0], this.mTime);    
    this.mInputTupleSchemaLocationsKnown = true;
  }

  /**
   * 
   * @param pSchema
   * @param pAttributeName
   * @return integer position of "attributeName" in "schema"
   * 
   */
  private int getAttributePosition(final TupleSchema pSchema, final String pAttributeName)
  {
    // TODO JMW: Throw exception if attributeName not found?
    return pSchema.getPosition(pAttributeName);
  }

  /**
   * 
   * @param pSchema
   * @param pTuple
   * @param pAttributeName
   * @return String representation tuple value
   * 
   *         Returns value of "attributeName" in "tuple"
   */
  private String getValue(final TupleSchema pSchema, final Tuple pTuple, final String pAttributeName)
  {
    String value;
    final int position = pSchema.getPosition(pAttributeName);
    value = ((BaseAttr) pTuple.getAttribute(position)).attrVal().toString();
    return value;
  }

  /**
   * 
   * @param pTuple
   * @param pAttributeLoc
   * @return String representation of "tuple" value at "attributeLoc"
   */
  private String getValue(final Tuple pTuple, final int pAttributeLoc)
  {
    String value;
    value = ((BaseAttr) pTuple.getAttribute(pAttributeLoc)).attrVal().toString();
    return value;
  }

  // We can punctuate on:
  // 1: Time and GroupBy Attribute
  // 2: Time
  // 3: GroupBy Attribute
  private long processPunctuationInAggregateFrameDetector(final FrameTuple pTuple)
  {
    ArrayList<FrameOutput> framesToOutput = new ArrayList<FrameOutput>();
    long punctuatedUpToTime = -1;

    // Case 1: Time and GroupBy Attribute (detectorid)
    if (!pTuple.getTimeAttr().equalsIgnoreCase("*") && !pTuple.getGroupbyAttr().equalsIgnoreCase("*"))
    {
      if (this.mDetectorsHashtable.containsKey(pTuple.getGroupbyAttr()))
      {
        framesToOutput = this.mDetectorsHashtable.get(pTuple.getGroupbyAttr()).processPunctuationOnTimeAttribute(pTuple);     
        framecount+=framesToOutput.size();
      }
    }
    // Case 2: Time
    else if (!pTuple.getTimeAttr().equalsIgnoreCase("*"))
    {
      // Update time for all hash tables
      final Enumeration<String> k = this.mDetectorsHashtable.keys();
      while (k.hasMoreElements())
      {
        final String key = k.nextElement();
        framesToOutput = this.mDetectorsHashtable.get(key).processPunctuationOnTimeAttribute(pTuple);   
        framecount+=framesToOutput.size();
      }
    }
    // Case 3: GroupBy Attribute (routerid)
    else if (!pTuple.getGroupbyAttr().equalsIgnoreCase("*"))
    {
      if (this.mDetectorsHashtable.containsKey(pTuple.getGroupbyAttr()))
      {
      }

      System.err.println("Punctuating on GroupBy Attribute not implemented.");
      System.exit(-1);
    }
    else
    {
      System.err.println("Invalid Punctuation: A Frame can only punctuate on Time or Group By Attributes.");
      System.exit(-1);
    }

    // Output frames we found
    for (int i = 0; i < framesToOutput.size(); i++)
    {
      this.outputFrame(framesToOutput.get(i));
      punctuatedUpToTime = Long.parseLong(framesToOutput.get(i).endTime);
    }
        
    return punctuatedUpToTime;

  }

  

  private void insertTupleIntoAggregateFrameDetector(final FrameTuple fTuple)
  {
    // See if we already have a frame for this groupby attribute
    if (this.mDetectorsHashtable.containsKey(fTuple.getGroupbyAttr()))
    {
      final DeltaFrameDetector detector = this.mDetectorsHashtable.get(fTuple.getGroupbyAttr());
      detector.insertTupleIntoFrame(fTuple);
    }
    else
    {
      final DeltaFrameDetector newDetector = new DeltaFrameDetector(fTuple, this.mDelta);
      this.mDetectorsHashtable.put(fTuple.getGroupbyAttr(), newDetector);
    }
  }

  private void outputFrame(final FrameOutput frameOut)
  {
    final Tuple outputTuple = new Tuple(false, 8);

    outputTuple.appendAttribute(new IntegerAttr(frameOut.frameId));
    outputTuple.appendAttribute(new TSAttr(frameOut.startTime));
    outputTuple.appendAttribute(new TSAttr(frameOut.endTime));
    outputTuple.appendAttribute(new TSAttr(frameOut.fragmentStart));
    outputTuple.appendAttribute(new TSAttr(frameOut.fragmentEnd));
    outputTuple.appendAttribute(new StringAttr(frameOut.frameStatus));
    outputTuple.appendAttribute(new IntegerAttr(frameOut.groupAttr));
    outputTuple.appendAttribute(new IntegerAttr(frameOut.length));


    try
    {
      this.putTuple(outputTuple, 0);
    }
    catch (final InterruptedException e)
    {
      e.printStackTrace();
    }
    catch (final ShutdownException e)
    {
      e.printStackTrace();
    }
  }

  private void outputPunctuation(final long pTime)
  {
    final Punctuation punctuationTuple = new Punctuation(false, 8);

    punctuationTuple.appendAttribute(BaseAttr.createWildStar(BaseAttr.Type.Int));
    punctuationTuple.appendAttribute(new TSAttr(Long.toString(pTime)));
    punctuationTuple.appendAttribute(BaseAttr.createWildStar(BaseAttr.Type.TS));
    punctuationTuple.appendAttribute(BaseAttr.createWildStar(BaseAttr.Type.TS));
    punctuationTuple.appendAttribute(BaseAttr.createWildStar(BaseAttr.Type.TS));
    punctuationTuple.appendAttribute(BaseAttr.createWildStar(BaseAttr.Type.String));
    punctuationTuple.appendAttribute(BaseAttr.createWildStar(BaseAttr.Type.Int));
    punctuationTuple.appendAttribute(BaseAttr.createWildStar(BaseAttr.Type.Int));

    try
    {
      this.putTuple(punctuationTuple, 0);
    }
    catch (final InterruptedException e)
    {
      e.printStackTrace();
    }
    catch (final ShutdownException e)
    {
      e.printStackTrace();
    }
  }

  void print(final String output)
  {
    if (NiagraServer.DEBUG && !this.OVERRIDE_DEBUG)
    {
      System.out.println(output);
    }
  }
  
//when stream is closed, then flush any frames still open in memory
  public void streamClosed(int streamId) throws ShutdownException {
	  FrameOutput frameToOutput = null;
	  long punctuatedUpToTime = -1;
	// Update time for all hash tables
      final Enumeration<String> k = this.mDetectorsHashtable.keys();
      while (k.hasMoreElements())
      {
        final String key = k.nextElement();
        frameToOutput = this.mDetectorsHashtable.get(key).flushOpenFrame();        
      }
      
      if(frameToOutput != null)
      {
          outputFrame(frameToOutput);
          punctuatedUpToTime = Long.parseLong(frameToOutput.endTime);
        }
	// Pass on punctuation.
	outputPunctuation(punctuatedUpToTime);
	System.out.println(framecount+" frames. (framecount)");
      }

}
