/**
 * 
 */
package it.unibz.inf.dis.network.components;

/**
 *
 * <p>The <code>Link</code> class</p>
 * <p>Copyright: 2006 - 2009 <a href="http://www.inf.unibz.it/dis">Dis Research Group</a></p>
 * <p> Domenikanerplatz -  Bozen, Italy.</p>
 * <p> </p>
 * @author <a href="mailto:markus.innerebner@inf.unibz.it">Markus Innerebner</a>.
 * @version 2.2
 */
public class Link implements Comparable<Link>{
  int startNodeId, endNodeId, id, originId,originStartNodeId,originEndNodeId;
  double length, originLength;
  double startOffset, endOffset;
  
  public static int NOT_SET=Integer.MIN_VALUE;
  
  /**
   * 
   * <p>Constructs a(n) <code>Link</code> object.</p>
   * @param linkId
   * @param startNodeId
   * @param endNodeId
   * @param length
   */
  public Link(int linkId, int startNodeId, int endNodeId, double length) {
    this.id = linkId;
    this.startNodeId = startNodeId;
    this.endNodeId = endNodeId;
    this.length = length;
    this.originStartNodeId = NOT_SET;
    this.originEndNodeId = NOT_SET;
    this.originLength = NOT_SET;
  }
  
  /**
   * 
   * <p>Constructs a(n) <code>Link</code> object.</p>
   * @param linkId
   * @param startNodeId
   * @param endNodeId
   * @param starOffset
   * @param originLinkId
   * @param originLength
   */
  public Link(int linkId, int startNodeId, int endNodeId, double startOffset, double endOffset, int originLinkId, double originLength) {
    this(linkId,startNodeId,endNodeId,endOffset - startOffset);
    this.startOffset = startOffset;
    this.endOffset = endOffset;
    this.originId = originLinkId;
    this.originLength = originLength;
  }
  
  
  public int getId() {
    return id;
  }
  
  public int getStartNodeId() {
    return startNodeId;
  }
  
  /**
   * 
   * <p>Method setStartNodeId</p> overrides the start node id, but backups it in old value in the origin start node variable
   * @param startNodeId
   */
  public void setStartNodeId(int startNodeId) {
    this.originStartNodeId = this.startNodeId;
    this.startNodeId = startNodeId;
  }
  
  public int getEndNodeId() {
    return endNodeId;
  }
  
  /**
   * 
   * <p>Method setEndNodeId</p> overrides the end node id, but backups it in old value in the origin end node variable
   * @param endNodeId
   */
  public void setEndNodeId(int endNodeId) {
    this.originEndNodeId = this.endNodeId;
    this.endNodeId = endNodeId;
  }
  
  public double getLength() {
    return length;
  }
  
  public void setLength(double length) {
    this.originLength = this.length;
    this.length = length;
  }
  
  public double getOriginLength() {
    return originLength;
  }
  
  public int getOriginId() {
    return originId;
  }
  
  public double getStartOffset() {
    return startOffset;
  }
  
  public void setStartOffset(double startOffset) {
    this.startOffset = startOffset;
  }
  
  public double getEndOffset() {
    return endOffset;
  }
  
  public void setEndOffset(double endOffset) {
    this.endOffset = endOffset;
  }
  
  @Override
  public String toString() {
    StringBuffer b = new StringBuffer();
    b.append("{ linkId: ").append(id);
    b.append(", startNodeId: ").append(startNodeId);
    b.append(", endNodeId: ").append(endNodeId);
    b.append(", startOffset: ").append(startOffset);
    b.append(", endOffset: ").append(endOffset);
    b.append(", length: ").append(length);
    b.append(", originLinkId: ").append(originId);
    b.append(", originLength: ").append(originLength);
    b.append("}");
    return b.toString();
  }
  
  @Override
  public int compareTo(Link o) {
    if(this.id<o.getId())return -1;
    if(this.id>o.getId())return 1;
    return 0;
  }
  
  

}
