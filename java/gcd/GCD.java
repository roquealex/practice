import java.util.Arrays;
public class GCD{
  public static int compute(int[] arr){
    return compRec(arr,0,arr.length);
  }

  private static int compRec(int[] arr, int start, int end){
    if (start+1 == end) return arr[start];
    int mid = (start+end)/2;
    int left = compRec(arr,start,mid);
    int right = compRec(arr,mid,end);

    return reduce(left,right);

  }

  private static int reduce(int left, int right) {
    if (right==0) return left;
    return(reduce(right, left%right));
  }

  public static void main(String s[]) {
    //int a[] = {15,10,5};
    //int a[] = {2,3,4,5,6};
    int a[] = {40,20,32,28,120};
    System.out.println(compute(a));
  }

}
