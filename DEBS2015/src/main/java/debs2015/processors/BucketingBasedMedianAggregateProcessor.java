package debs2015.processors;

/**
* Created by sachini on 1/9/15.
*/
public class BucketingBasedMedianAggregateProcessor {
//    int multiplexer =1;
    int size = 10002 ;
    int[] mediationArray = new int[size];
    int totalElements = 0;

    float lastReturnedMedian = 0;

    int lastBucketIndex;
    int bucketChainIndex;

    public float getMedian() {

        if (totalElements % 2 == 0) {
            int firstMedianIndex = ((totalElements) / 2);
            int secondMedianIndex = ((totalElements) / 2) + 1;

            int firstMedianValue = 0;
            int secondMedianValue = 0;
            boolean flag = true;

            int count = 0;
            int loopCount = 0;
            for (int occurrenceCount : mediationArray) {
                count = count + occurrenceCount;

                if (firstMedianIndex <= count && flag) {
                    firstMedianValue = loopCount;
                    flag = false;
                    loopCount++;
                    continue;
                }
                if (secondMedianIndex <= count) {
                    secondMedianValue = loopCount;
                    break;
                }
                loopCount++;
            }
            lastReturnedMedian =  (firstMedianValue + secondMedianValue) / 2f;


        } else {
            int medianIndex = ((totalElements - 1) / 2) + 1;
            int count = 0;
            int medianValue = 0;
            int loopCount = 0;
            for (int medianCount : mediationArray) {
                count = count + medianCount;
                if (medianIndex <= count) {
                    medianValue = loopCount;
                    break;
                }
                loopCount++;
            }
            lastReturnedMedian =  medianValue;
        }

        lastReturnedMedian = lastReturnedMedian/100;
        return lastReturnedMedian;
    }

            //10001
    public float processAdd(float element) {
        if(element<0){
              return lastReturnedMedian;
         } else {
            int roundValue = (int) (element*100);
              if (roundValue < size - 1) {
                 mediationArray[roundValue] += 1;
             } else {
                 roundValue = 10001;
                 mediationArray[size - 1] += 1;
             }
             totalElements++;

             return addAndGetMedian(roundValue);
         }
    }

    private float addAndGetMedian(int roundValue){
        float returnVal;
        if(totalElements==1){
            lastBucketIndex = roundValue;
            bucketChainIndex = 1;
            returnVal =  roundValue;
        } else if(totalElements%2 == 0){ // even
             if(lastBucketIndex>roundValue){
                 returnVal = (lastBucketIndex+getBefore())/200; //Done
             } else {
                 returnVal =  (lastBucketIndex+getNext(false))/200;  //done
             }
        }  else {
            if(lastBucketIndex>roundValue){
                returnVal =  (lastBucketIndex)/100;  //done
            } else {
                returnVal =  (getNext())/100;   //done
            }
        }

        lastReturnedMedian = returnVal;
        return returnVal;
    }



    private int getNext(){
      if(mediationArray[lastBucketIndex]>bucketChainIndex){
                  bucketChainIndex++;
                  return  lastBucketIndex;
      }  else {
          for(int i= lastBucketIndex+1;i<size;i++){
              if(mediationArray[i]>0) {
                  lastBucketIndex = i;
                  bucketChainIndex = 1;
                  return i;
              }
          }
      }
        return  lastBucketIndex;
    }

    private int getNext(boolean keepLastValues){
        if(mediationArray[lastBucketIndex]>bucketChainIndex){
            return  lastBucketIndex;
        }  else {
            for(int i= lastBucketIndex+1;i<size;i++){
                if(mediationArray[i]>0) {
                    return i;
                }
            }
        }
        return  lastBucketIndex;
    }


    private int getBefore(){
        if(bucketChainIndex>1){
            bucketChainIndex--;
            return  lastBucketIndex;
        }  else {
            for(int i= lastBucketIndex-1;i<size;i--){
                if(mediationArray[i]>0) {
                    lastBucketIndex = i;
                    bucketChainIndex = mediationArray[i];
                    return i;
                }
            }
        }
        return  lastBucketIndex;
    }


    public float processRemove(float element) {
        if(element<0){
            return lastReturnedMedian;
        } else {
            int roundValue = (int) (element*100);
            if (roundValue < size - 1) {
                mediationArray[roundValue] -= 1;
            } else {
                roundValue = 10001;
                mediationArray[size - 1] -= 1;
            }
            totalElements--;

            return removeAndGetMedian(roundValue);
        }
    }


    private float removeAndGetMedian(int roundValue){
        float returnVal;
        if(totalElements==0){
            lastBucketIndex = 0;
            bucketChainIndex = 0;
            returnVal =  0;
        } else if(totalElements%2 == 0){ // even
            if(lastBucketIndex<roundValue){
                returnVal = (lastBucketIndex+getBefore())/200; //Done
            } else {
                returnVal =  (lastBucketIndex+getNext(false))/200;  //done
            }
        }  else {
            if(lastBucketIndex<roundValue){
                returnVal =  (lastBucketIndex)/100;  //done
            } else {
                returnVal =  (getNext())/100;   //done
            }
        }

        lastReturnedMedian = returnVal;
        return returnVal;
    }

//    public void removeElement(float element) {
//        int roundValue = Math.round(element*multiplexer);
//        if (roundValue < size - 1) {
//            mediationArray[roundValue] -= 1;
//        } else {
//            mediationArray[size - 1] -= 1;
//        }
//        totalElements--;
//    }



}
