package simpledb.optimizer;

import simpledb.execution.Aggregate;
import simpledb.execution.Aggregator;
import simpledb.execution.Predicate;

import java.util.Arrays;

/** A class to represent a fixed-width histogram over a single integer-based field.
 */
public class IntHistogram {

    private int buckets;  //桶数量
    private int min;    //当前field最小值
    private int max; //当前field最大值
    private double avg;  //平均每个桶表示的值数量，其实是一个整数
    private MyGram[] myGrams;  //每一个桶
    private int ntups;  //一共的tuple数量


    public class MyGram{
        private double  left;    //当前gram左边界
        private double right;    //当前gram右边界
        private double w;      //当前gram宽度
        private int count;     //当前gram的tuple个数

        public MyGram(double left,double right){
            this.left = left;
            this.right = right;
            this.w = right - left;
            this.count = 0;
        }

        /**
         * 判断某个值是否在该gram中，每一个gram是左开右闭的
         * @param v
         * @return
         */
        public boolean isInRange(int v){
            if(v>=left && v<right){
                return true;
            }
            return false;
        }

        @Override
        public String toString() {
            return "MyGram{" +
                    "left=" + left +
                    ", right=" + right +
                    ", w=" + w +
                    ", count=" + count +
                    '}';
        }

    }

    /**
     * Create a new IntHistogram.
     * 
     * This IntHistogram should maintain a histogram of integer values that it receives.
     * It should split the histogram into "buckets" buckets.
     * 
     * The values that are being histogrammed will be provided one-at-a-time through the "addValue()" function.
     * 
     * Your implementation should use space and have execution time that are both
     * constant with respect to the number of values being histogrammed.  For example, you shouldn't 
     * simply store every value that you see in a sorted list.
     * 
     * @param buckets The number of buckets to split the input value into.
     * @param min The minimum integer value that will ever be passed to this class for histogramming
     * @param max The maximum integer value that will ever be passed to this class for histogramming
     */
    public IntHistogram(int buckets, int min, int max) {
    	// some code goes here
        this.buckets = buckets;
        this.min=min;
        this.max = max;
        this.avg = ((max-min)*1.0)/(buckets*1.0);
        this.myGrams = new MyGram[buckets];
        this.ntups = 0;

        if(this.avg %1!=0){
            this.avg = (int)(this.avg+1);
        }
        double l = min;
        for(int i=0;i<buckets;i++){
            myGrams[i] = new MyGram(l,l+this.avg);
            l = l + avg;
        }
    }

    private int binarySearch(int v){
        int l = 0;
        int r = buckets-1;

        while(l<=r){
            int mid = (l+r)/2;
            if(myGrams[mid].isInRange(v)){
                return mid;
            }else if(myGrams[mid].left>v){
                r  = mid -1;
            }else if(myGrams[mid].right<=v){
                l = mid+1;
            }
        }
        return -1;
    }

    /**
     * Add a value to the set of values that you are keeping a histogram of.
     * @param v Value to add to the histogram
     */
    public void addValue(int v) {
    	// some code goes here
        int target = binarySearch(v);
        if(target!=-1){
            myGrams[target].count++;
            ntups++;
        }
    }

    /**
     * Estimate the selectivity of a particular predicate and operand on this table.
     * 
     * For example, if "op" is "GREATER_THAN" and "v" is 5, 
     * return your estimate of the fraction of elements that are greater than 5.
     * 
     * @param op Operator
     * @param v Value
     * @return Predicted selectivity of this particular operator and value
     */
    public double estimateSelectivity(Predicate.Op op, int v) {
    	// some code goes here
        int target = binarySearch(v);
        MyGram cur;
        if(target!=-1){
            cur = myGrams[target];
        }else{
            cur = null;
        }

        if(op == Predicate.Op.EQUALS){
            if(cur==null){
                return 0.0;
            }
            return (cur.count/cur.w)/(ntups*1.0);
        }else if(op == Predicate.Op.GREATER_THAN){
            if(v<min){
                return 1.0;
            }else if(v>=max){
                return 0.0;
            }else if(cur!=null){
                double res = ((cur.right-v)/cur.w)*(cur.count*1.0)/(ntups*1.0);
                for(int i  =target+1;i<buckets;i++){
                    res += (myGrams[i].count *1.0)/(ntups*1.0);
                }
                return res;
            }
        }else if(op == Predicate.Op.LESS_THAN){
            if(v<=min){
                return 0.0;
            }else if(v >max){
                return 1.0;
            }else if (cur!=null){
                double res =  ((v-cur.left)/cur.w)*(cur.count*1.0)/(ntups*1.0);
                for(int i=0;i<target;i++){
                    res += (myGrams[i].count*1.0)/(ntups*1.0);
                }
                return res;
            }
        }else if(op == Predicate.Op.NOT_EQUALS){
            if(cur==null){
                return 1.0;
            }
            return 1-((cur.count/cur.w)/(ntups*1.0));
        }else if(op == Predicate.Op.GREATER_THAN_OR_EQ){
            if(v<=min){
                return 1.0;
            }else if(v>max){
                return 0.0;
            }else if(cur!=null){
                double res = ((cur.right-v+1)/cur.w)*(cur.count*1.0)/(ntups*1.0);
                for(int i  =target+1;i<buckets;i++){
                    res += (myGrams[i].count *1.0)/(ntups*1.0);
                }
                return res;
            }
        }else if(op == Predicate.Op.LESS_THAN_OR_EQ){
            if(v<min){
                return 0.0;
            }else if(v >=max){
                return 1.0;
            }else if (cur!=null){
                double res =  ((v-cur.left+1)/cur.w)*(cur.count*1.0)/(ntups*1.0);
                for(int i=0;i<target;i++){
                    res += (myGrams[i].count*1.0)/(ntups*1.0);
                }
                return res;
            }
        }

        return 0.0;
    }
    
    /**
     * @return
     *     the average selectivity of this histogram.
     *     
     *     This is not an indispensable method to implement the basic
     *     join optimization. It may be needed if you want to
     *     implement a more efficient optimization
     * */
    public double avgSelectivity()
    {
        // some code goes here
        return avg;
    }
    
    /**
     * @return A string describing this histogram, for debugging purposes
     */
    @Override
    public String toString() {
        // some code goes here
        return "IntHistogram{" +
                "buckets=" + buckets +
                ", min=" + min +
                ", max=" + max +
                ", avg=" + avg +
                ", myGrams=" + Arrays.toString(myGrams) +
                '}';
    }
}
