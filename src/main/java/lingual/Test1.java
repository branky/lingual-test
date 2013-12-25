package lingual;

import cascading.flow.Flow;
import cascading.flow.FlowDef;
import cascading.flow.hadoop.HadoopFlowConnector;
import cascading.lingual.flow.SQLPlanner;
import cascading.lingual.tap.hadoop.SQLTypedTextDelimited;
import cascading.tap.SinkMode;
import cascading.tap.Tap;
import cascading.tap.hadoop.Hfs;
import cascading.tuple.TupleEntryIterator;

import java.io.IOException;

/**
 */
public class Test1 {
    public static void main( String[] args ) throws Exception
    {
        new Test1().run();
    }

    public void run() throws IOException
    {
        String statement = "select a.\"NAME\", sum(s.\"SALE_AMT\")\n"
                + "from \"example\".\"sales_fact_1997\" as s\n"
                + "join \"example\".\"area\" as a\n"
                + "on a.\"ID\" = s.\"AREA_ID\"\n"
                + "where s.\"SALE_QTR\" in ('Q1', 'Q2') "
                + "group by a.\"NAME\"";

        Tap empTap = new Hfs( new SQLTypedTextDelimited( ",", "\"" ),
                "data/area.tcsv", SinkMode.KEEP );
        Tap salesTap = new Hfs( new SQLTypedTextDelimited( ",", "\"" ),
                "data/sales_fact_1997.tcsv", SinkMode.KEEP );

        Tap resultsTap = new Hfs( new SQLTypedTextDelimited( ",", "\"" ),
                "output/flow1/results.tcsv", SinkMode.REPLACE );

        FlowDef flowDef = FlowDef.flowDef()
                .setName( "sql flow" )
                .addSource( "example.area", empTap )
                .addSource( "example.sales_fact_1997", salesTap )
                .addSink( "results", resultsTap );

        SQLPlanner sqlPlanner = new SQLPlanner()
                .setSql( statement );

        flowDef.addAssemblyPlanner( sqlPlanner );

        Flow flow = new HadoopFlowConnector().connect( flowDef );

        flow.complete();

        TupleEntryIterator iterator = resultsTap.openForRead( flow.getFlowProcess() );

        flow.writeDOT("flow1.dot");

        while( iterator.hasNext() )
            System.out.println( iterator.next() );

        iterator.close();
    }
}
