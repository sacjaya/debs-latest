package debs2015.processors;

import org.apache.log4j.Logger;


public class CellIdProcessorQ1  {
    private Logger log = Logger.getLogger(CellIdProcessorQ1.class);

    private float  westMostLongitude = -74.916578f; //previous -74.916586f;
    private float  eastMostLongitude = -73.120778f; //previous -73.116f;
    private float  northMostLatitude = 41.477182778f; //previous 41.477185f;
    private float  southMostLatitude = 40.129715978f; //previous 40.128f;

    private float  longitudeDifference = eastMostLongitude-westMostLongitude ;
    private float  latitudeDifference = northMostLatitude-southMostLatitude ;
    private short  gridResolution = 300; //This is the number of cells per side in the square of the simulated area.

    public int execute(float inputLongitude, float inputLatitude) {
        //--------------------------------------------------          The following is for longitude -------------------------------------

        short cellIdFirstComponent;

        if(westMostLongitude==inputLongitude){
            cellIdFirstComponent= 1;
        } else {
            cellIdFirstComponent = (short) Math.ceil((((inputLongitude- westMostLongitude) / longitudeDifference) * gridResolution));
        }

        //--------------------------------------------------          The following is for latitude -------------------------------------

        short cellIdSecondComponent;

        if(northMostLatitude == inputLatitude) {
            cellIdSecondComponent = 1;
        } else {
            cellIdSecondComponent = (short) Math.ceil((((northMostLatitude - inputLatitude) / latitudeDifference) * gridResolution));
        }

//        System.out.println("*"+cellIdFirstComponent*1000+cellIdSecondComponent);
        //return  (cellIdFirstComponent*601+cellIdSecondComponent);
        return  (cellIdFirstComponent*301+cellIdSecondComponent);
    }
}
