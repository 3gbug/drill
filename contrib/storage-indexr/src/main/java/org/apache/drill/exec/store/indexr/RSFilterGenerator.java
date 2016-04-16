package org.apache.drill.exec.store.indexr;

import io.indexr.segment.ColumnSchema;
import io.indexr.segment.ColumnType;
import io.indexr.segment.SegmentSchema;
import io.indexr.segment.rc.*;
import io.indexr.util.Trick;
import org.apache.calcite.sql.*;
import org.apache.calcite.sql.util.SqlVisitor;

import java.util.List;

public class RSFilterGenerator implements SqlVisitor<RCOperator> {
  private final SegmentSchema tableSchema;

  public RSFilterGenerator(SegmentSchema tableSchema) {
    this.tableSchema = tableSchema;
  }

  public RCOperator gen(SqlNode filter) {
    return filter.accept(this).optimize();
  }

  private Attr toAttr(SqlNode node) {
    assert node instanceof SqlIdentifier;
    SqlIdentifier identifier = (SqlIdentifier) node;
    return new Attr(identifier.names.get(identifier.names.size() - 1));
  }

  private ColumnSchema toColumn(String fieldName) {
    for (ColumnSchema cs : tableSchema.columns) {
      if (cs.name.equalsIgnoreCase(fieldName)) {
        return cs;
      }
    }
    throw new RuntimeException(String.format("Field %s not found!", fieldName));
  }

  private long toNumLiteral(ColumnSchema cs, SqlNode node) {
    assert node instanceof SqlLiteral;
    SqlLiteral num = (SqlLiteral) node;
    switch (cs.dataType) {
      case ColumnType.INT:
      case ColumnType.LONG:
        return Long.parseLong(num.toValue());
      case ColumnType.FLOAT:
      case ColumnType.DOUBLE:
        return Double.doubleToRawLongBits(Double.parseDouble(num.toValue()));
      default:
        throw new RuntimeException();
    }
  }

  private String toStringLiteral(SqlNode node) {
    assert node instanceof SqlLiteral;
    return ((SqlLiteral) node).toValue();
  }

  private LongAndString toValue(Attr attr, SqlNode node) {
    ColumnSchema cs = toColumn(attr.colName);
    LongAndString value = new LongAndString();
    if (cs.dataType == ColumnType.STRING) {
      value.str = toStringLiteral(node);
    } else {
      value.num = toNumLiteral(cs, node);
    }
    return value;
  }

  private AttrAndValue toAttrAndValue(SqlNode n1, SqlNode n2) {
    SqlIdentifier idNode;
    SqlLiteral valueNode;
    if (n1 instanceof SqlIdentifier && n2 instanceof SqlLiteral) {
      idNode = (SqlIdentifier) n1;
      valueNode = (SqlLiteral) n2;
    } else if (n2 instanceof SqlIdentifier && n1 instanceof SqlLiteral) {
      idNode = (SqlIdentifier) n2;
      valueNode = (SqlLiteral) n1;
    } else {
      return null;
    }
    AttrAndValue av = new AttrAndValue();
    av.attr = toAttr(idNode);
    LongAndString value = toValue(av.attr, valueNode);
    av.num = value.num;
    av.str = value.str;
    return av;
  }

  @Override
  public RCOperator visit(SqlCall call) {
    switch (call.getKind()) {
      case AND:
        return new And(Trick.mapToList(call.getOperandList(), n -> n.accept(this)));
      case OR:
        return new Or(Trick.mapToList(call.getOperandList(), n -> n.accept(this)));
      case NOT:
        return new Not(call.operand(0).accept(this));
      case BETWEEN: {
        Attr attr = toAttr(call.operand(0));
        LongAndString v1 = toValue(attr, call.operand(1));
        LongAndString v2 = toValue(attr, call.operand(2));
        return new Between(attr, v1.num, v2.num, v1.str, v2.str);
      }
      case EQUALS: {
        AttrAndValue av = toAttrAndValue(call.operand(0), call.operand(1));
        return new Equal(av.attr, av.num, av.str);
      }
      case NOT_EQUALS: {
        AttrAndValue av = toAttrAndValue(call.operand(0), call.operand(1));
        return new NotEqual(av.attr, av.num, av.str);
      }
      case GREATER_THAN: {
        AttrAndValue av = toAttrAndValue(call.operand(0), call.operand(1));
        return new Greater(av.attr, av.num, av.str);
      }
      case GREATER_THAN_OR_EQUAL: {
        AttrAndValue av = toAttrAndValue(call.operand(0), call.operand(1));
        return new GreaterEqual(av.attr, av.num, av.str);
      }
      case IN: {
        // TODO
        Attr attr = toAttr(call.operand(0));
        SqlNode vns = call.operand(1);
        assert vns instanceof SqlNodeList;
        SqlNodeList nl = (SqlNodeList) vns;
        List<LongAndString> values = Trick.mapToList(nl.getList(), n -> toValue(attr, n));
        long[] nums = new long[values.size()];
        String[] strs = new String[values.size()];
        for (int i = 0; i < values.size(); i++) {
          nums[i] = values.get(i).num;
          strs[i] = values.get(i).str;
        }
        return new In(attr, nums, strs);
      }
      case LESS_THAN: {
        AttrAndValue av = toAttrAndValue(call.operand(0), call.operand(1));
        return new Less(av.attr, av.num, av.str);
      }
      case LESS_THAN_OR_EQUAL: {
        AttrAndValue av = toAttrAndValue(call.operand(0), call.operand(1));
        return new LessEqual(av.attr, av.num, av.str);
      }
      default:
        return new UnknownOperator(call.toString());
    }
  }

  @Override
  public RCOperator visit(SqlNodeList nodeList) {
    return notSupport(nodeList);
  }

  @Override
  public RCOperator visit(SqlLiteral literal) {
    return notSupport(literal);
  }

  @Override
  public RCOperator visit(SqlIdentifier id) {
    return notSupport(id);
  }

  @Override
  public RCOperator visit(SqlDataTypeSpec type) {
    return notSupport(type);
  }

  @Override
  public RCOperator visit(SqlDynamicParam param) {
    return notSupport(param);
  }

  @Override
  public RCOperator visit(SqlIntervalQualifier intervalQualifier) {
    return notSupport(intervalQualifier);
  }

  private RCOperator notSupport(SqlNode node) {
    return new UnknownOperator(node.toString());
  }

  private static class LongAndString {
    public long num;

    public String str;
  }

  private static class AttrAndValue {
    public Attr attr;

    public long num;

    public String str;
  }
}
