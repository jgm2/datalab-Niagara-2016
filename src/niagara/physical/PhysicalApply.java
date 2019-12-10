package niagara.physical;

import java.util.ArrayList;

import niagara.logical.Apply;
import niagara.logical.Variable;
import niagara.logical.predicates.Predicate;
import niagara.optimizer.colombia.Attribute;
import niagara.optimizer.colombia.Cost;
import niagara.optimizer.colombia.ICatalog;
import niagara.optimizer.colombia.LogicalOp;
import niagara.optimizer.colombia.LogicalProperty;
import niagara.optimizer.colombia.Op;
import niagara.optimizer.colombia.PhysicalProperty;
import niagara.physical.predicates.PredicateImpl;
import niagara.query_engine.TupleSchema;
import niagara.utils.ControlFlag;
import niagara.utils.IntegerAttr;
import niagara.utils.Punctuation;
import niagara.utils.ShutdownException;
import niagara.utils.Tuple;

/**
 * Implementation of the Append2 operator.
 */

@SuppressWarnings("unchecked")
public class PhysicalApply extends PhysicalOperator
{
  // No blocking source streams
  private static final boolean[] blockingSourceStreams = { false };

  // The is the predicate to apply to the tuples
  private Predicate              pred;
  private PredicateImpl          predEval;
  private String                 applyAttr;
  
  String                         guardOutput           = "*";
  String                         fAttr;

  public PhysicalApply()
  {
    setBlockingSourceStreams(blockingSourceStreams);
  }

  public void opInitFrom(LogicalOp logicalOperator)
  {
    pred = ((Apply) logicalOperator).getPredicate();
    predEval = pred.getImplementation();
    applyAttr = ((Apply) logicalOperator).getApplyAttr();
  }

  public Op opCopy()
  {
    PhysicalApply p = new PhysicalApply();
    p.pred = pred;
    p.predEval = predEval;
    p.applyAttr = applyAttr;
    return p;
  }

  void processCtrlMsgFromSink(ArrayList ctrl, int streamId)
      throws java.lang.InterruptedException, ShutdownException
  {
    // downstream control message is GET_PARTIAL
    // We should not get SYNCH_PARTIAL, END_PARTIAL, EOS or NULLFLAG
    // REQ_BUF_FLUSH is handled inside SinkTupleStream
    // here (SHUTDOWN is handled with exceptions)

    if (ctrl == null)
      return;

    ControlFlag ctrlFlag = (ControlFlag) ctrl.get(0);

    switch (ctrlFlag)
    {
      case GET_PARTIAL:
        processGetPartialFromSink(streamId);
        break;
      case MESSAGE:
        // System.err.println(this.getName() + "***Got message: "
        // + ctrl.get(1));

        String[] feedback = ctrl.get(1).toString().split("#");

        fAttr = feedback[0];
        guardOutput = feedback[1];

        break;
      default:
        assert false : "KT unexpected control message from sink "
            + ctrlFlag.flagName();
    }
  }

  /**
   * This function processes a tuple element read from a source stream when
   * the operator is non-blocking. This over-rides the corresponding function
   * in the base class.
   * 
   * @param tupleElement
   *          The tuple element read from a source stream
   * @param streamId
   *          The source stream from which the tuple was read
   * 
   * @exception ShutdownException
   *              query shutdown by user or execution error
   */

  protected void processTuple(Tuple inputTuple, int streamId)
      throws ShutdownException, InterruptedException
  {    
    // Evaluate the predicate on the desired attribute of the tuple
    if (predEval.evaluate(inputTuple, null))
    {
      // Add a new integer attribute with a value of 1 (predicate is true)
      IntegerAttr applyAttr = new IntegerAttr(1);
      inputTuple.appendAttribute(applyAttr);
    }
    else
    {
      // Add a new integer attribute with a value of 0 (predicate is false)
      IntegerAttr applyAttr = new IntegerAttr(0);
      inputTuple.appendAttribute(applyAttr);
    }
    
    
    if (guardOutput.equals("*"))
    {
      // Don't drop any of the tuples
      putTuple(inputTuple, 0);
    }
    else
    {
      int pos = outputTupleSchema.getPosition(fAttr);
      IntegerAttr v = (IntegerAttr) inputTuple.getAttribute(pos);
      String tupleGuard = v.toASCII();
      // System.err.println("Read: " + tupleGuard);

      if (guardOutput.equals(tupleGuard))
      {
        if (predEval.evaluate(inputTuple, null))
          putTuple(inputTuple, 0);
        // System.out.println(this.getName() + "produced a tuple.");

        // System.err.println("Allowed production of tuple with value: "
        // + tupleGuard);
      }
      else
      {
        // putTuple(tupleElement,0);
        // System.err.println("Avoided production of tuple with value: "
        // + tupleGuard);
        // System.err.println(this.getName() +
        // "avoided sending a tuple.");
      }
    }

  }

  /**
   * This function processes a punctuation element read from a source stream
   * when the operator is non-blocking. This over-rides the corresponding
   * function in the base class.
   * 
   * Punctuations can simply be sent to the next operator from Select
   * 
   * @param inputTuple
   *          The tuple element read from a source stream
   * @param streamId
   *          The source stream from which the tuple was read
   * 
   * @exception ShutdownException
   *              query shutdown by user or execution error
   */
  protected void processPunctuation(Punctuation inputTuple, int streamId)
      throws ShutdownException, InterruptedException
  {
    putTuple(inputTuple, 0);
  }

  public boolean isStateful()
  {
    return false;
  }

  /**
   * @see niagara.optimizer.colombia.PhysicalOp#FindLocalCost(ICatalog,
   *      LogicalProperty, LogicalProperty[])
   */
  public Cost findLocalCost(ICatalog catalog, LogicalProperty[] InputLogProp)
  {
    float InputCard = InputLogProp[0].getCardinality();
    Cost cost = new Cost(InputCard * catalog.getDouble("tuple_reading_cost"));
    cost.add(predEval.getCost(catalog).times(InputCard));
    return cost;
  }

  public boolean equals(Object o)
  {
    if (o == null || !(o instanceof PhysicalApply))
      return false;
    if (o.getClass() != PhysicalApply.class)
      return o.equals(this);
    return pred.equals(((PhysicalApply) o).pred);
  }

  /**
   * @see java.lang.Object#hashCode()
   */
  public int hashCode()
  {
    return pred.hashCode();
  }

  /**
   * @see niagara.optimizer.colombia.PhysicalOp#FindPhysProp(PhysicalProperty[])
   */
  public PhysicalProperty findPhysProp(PhysicalProperty[] input_phys_props)
  {
    return input_phys_props[0];
  }

  /**
   * @see niagara.query_engine.SchemaProducer#constructTupleSchema(TupleSchema[])
   */
  public void constructTupleSchema(TupleSchema[] inputSchemas)
  {
    inputTupleSchemas = inputSchemas;
    outputTupleSchema = inputTupleSchemas[0];
    outputTupleSchema.addMapping(new Variable(applyAttr));
  }

  /**
   * @see niagara.query_engine.PhysicalOperator#opInitialize()
   */
  protected void opInitialize()
  {
    predEval.resolveVariables(inputTupleSchemas[0], 0);
  }

  /**
   * @see niagara.utils.SerializableToXML#dumpChildrenInXML(StringBuffer)
   */
  public void dumpChildrenInXML(StringBuffer sb)
  {
    sb.append(">");
    pred.toXML(sb);
    sb.append("</").append(getName()).append(">");
  }
}
