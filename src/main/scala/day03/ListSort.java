package day03;

import java.util.Arrays;

import org.junit.Test;

public class ListSort {
	
	public void maopao() {
		int tmp = 0;
		int a[] = { 49, 38, 65, 97, 76, 13, 27, 49, 78, 34, 12, 64, 5, 4, 62, 99, 98, 54, 56, 17, 18, 23, 34, 15, 35,
				25, 53, 51 };
		for (int i = 0; i < a.length - 1; i++) {

			for (int j = 0; j < a.length - 1 - i; j++) {

				if (a[j] < a[j + 1]) {
					tmp = a[j];
					a[j] = a[j + 1];
					a[j + 1] = tmp;

				}
			}

		}

		for (int i = 0; i < a.length; i++) {
			System.out.print(a[i] + ",");
		}

	}

	
	public void insertSort(){  
	    int a[]={49,38,65,97,76,13,27,49,78,34,12,64,5,4,62,99,98,54,56,17,18,23,34,15,35,25,53,51};  
	    int temp=0;  
	    for(int i=1;i<a.length;i++){  
	       int j=i-1;  
	       temp=a[i];  
	       for(;j>=0&&temp<a[j];j--){  
	           a[j+1]=a[j];  //将大于temp的值整体后移一个单位  
	       }  
	       a[j+1]=temp;  
	    }  
	   
	    for(int i=0;i<a.length;i++){  
	       System.out.print(a[i]+",");  
	    }  
	}

	
   public void test1() {
	    int a[]={49,38,65,97,76,13,27,49,78,34,12,64,5,4,62,99,98,54,56,17,18,23,34,15,35,25,53,51};  
     int tmp=0;
     for (int i = 0; i < a.length-1; i++) {
		
    	 for (int j = 0; j < a.length-1-i; j++) {
			   if(a[j]>a[j+1]) {
				   tmp=a[j];
				    a[j]=a[j+1];
				   	a[j+1]=tmp;			   
			   }
    		 
    		 
		}
    	 
	}
     for(int i=0;i<a.length;i++){  
	       System.out.print(a[i]+"|");  
	    }  
	  
   }
   @Test
   public void test2() {
	    int a[]={49,38,65,97,76,13,27,49,78,34,12,64,5,4,62,99,98,54,56,17,18,23,34,15,35,25,53,51};  

	     
	   System.out.println(Arrays.toString(a));
	   
   }

}
