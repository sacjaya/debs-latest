package debs2015.processors;

import org.apache.log4j.Logger;


public class CellIdProcessorQ2  {
    private Logger log = Logger.getLogger(CellIdProcessorQ2.class);

    private float  westMostLongitude = -74.9150815f; //previous -74.916586f;
    private float  eastMostLongitude = -73.1192815f; //previous -73.116f;
    private float  northMostLatitude = 41.476059889f; //previous 41.477185f;
    private float  southMostLatitude = 40.128593089f; //previous 40.128f;

    private float  longitudeDifference = eastMostLongitude-westMostLongitude ;
    private float  latitudeDifference = northMostLatitude-southMostLatitude ;
    private short  gridResolution = 600; //This is the number of cells per side in the square of the simulated area.

    public int execute(float inputLongitude, float inputLatitude) {
        //--------------------------------------------------          The following is for longitude -------------------------------------

        short cellIdFirstComponent;

        if(westMostLongitude==inputLongitude){
            cellIdFirstComponent= 1;
        } else {
            cellIdFirstComponent = (short) Math.ceil((((inputLongitude - westMostLongitude) / longitudeDifference) * gridResolution));
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
        return  (cellIdFirstComponent*601+cellIdSecondComponent);
    }
}
