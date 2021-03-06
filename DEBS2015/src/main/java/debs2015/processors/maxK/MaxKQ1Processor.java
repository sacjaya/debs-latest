/*
* Copyright (c) 2015, WSO2 Inc. (http://www.wso2.org)
* All Rights Reserved.
*
* WSO2 Inc. licenses this file to you under the Apache License,
* Version 2.0 (the "License"); you may not use this file except
* in compliance with the License.
* You may obtain a copy of the License at
*
*     http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing,
* software distributed under the License is distributed on an
* "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
* KIND, either express or implied.  See the License for the
* specific language governing permissions and limitations
* under the License.
*/

package debs2015.processors.maxK;


import debs2015.DebsEvent;

import java.util.LinkedList;

public class MaxKQ1Processor {

    private static final String NULL_VALUE = "NULL";
    //The K value
    private int kValue = 10;
    private MaxKStoreQ1 maxKStore = new MaxKStoreQ1();


    public Object[] process(DebsEvent debsEvent) {
       return processEventForMaxK(debsEvent.isCurrent(), debsEvent.getStartCellNoQ1(), debsEvent.getEndCellNoQ1());
    }

    private Object[] processEventForMaxK(boolean isCurrent, int startCell, int endCell) {


        Object[] data = new Object[kValue];


        int startCellValue = startCell;
        int endCellValue = endCell;


        LinkedList<Long> currentTopK;

        //The method getMaxK() accepts the "<start cell ID>:<end cell ID>" and the trip count found for this route.
        currentTopK = maxKStore.getMaxK(((long) (startCellValue)) * 100000 + (long) endCellValue, isCurrent, kValue);

        if (currentTopK == null) {
            return null;
        } else {

            //From here onwards we prepare the output data tuple from this operator.
            int position = 0;

            //This will be restricted to to k number of lists. Therefore, we do not need to check whether we have exceeded the top k.
            //We do this until top-k is 10 (kValue==10)
            for (long cell : currentTopK) {
                //String[] splitValues = cell.split(":");
                //System.out.println("cell|"+cell+"|"+cell.substring(0, colonIndex)+"|"+cell.substring(colonIndex+1));
                long startCellIntValue = cell / 100000;
                long endCellIntValue = cell % 100000;
                data[position++] = (startCellIntValue / 301) + "." + (startCellIntValue % 301) + "," + (endCellIntValue / 301) + "." + (endCellIntValue % 301);//profitable_cell_id_
            }

            //Populating remaining elements for the payload of the stream with null if we could not find the top-k number of routes.
            while (position < kValue) {
                data[position++] = NULL_VALUE;
            }

            return data;
        }

    }


}
