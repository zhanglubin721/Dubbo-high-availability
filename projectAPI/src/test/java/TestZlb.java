import com.yicaida.projectAPI.pojo.User;
import org.junit.Test;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.concurrent.locks.ReentrantLock;

/**
 * @author zhanglubin
 * @date 2021/1/21
 */
public class TestZlb {


//    冒泡排序（把大的挪后面）
    @Test
    public void test1() {
        LinkedList<Integer> initializeData = getInitializeData();
        Integer temporaryNum;
        int counterNum = 0;
        for (int i = 0; i < initializeData.size(); i++) {
            for (int j = 0; j < initializeData.size() - 1 - i; j++) {
                if (initializeData.get(j + 1) < initializeData.get(j)) {
                    temporaryNum = initializeData.get(j + 1);
                    initializeData.set(j + 1, initializeData.get(j));
                    initializeData.set(j, temporaryNum);
                    counterNum ++;
                }
            }
        }
        for (Integer initializeDatum : initializeData) {
            System.out.println(initializeDatum);
        }
        System.out.println("次数：" + counterNum);
    }

//    选择排序（把最小的挪前面）
    @Test
    public void test2() {
        LinkedList<Integer> initializeData = getInitializeData();
        Integer currentMinNum;
        int currentMinNumIndex;
        boolean currentMinNumIsChange;
        int counterNum = 0;
        for (int i = 0; i < initializeData.size(); i++) {
            currentMinNumIsChange = false;
            currentMinNum = initializeData.get(i);
            currentMinNumIndex = i;
            for (int j = i + 1; j < initializeData.size(); j++) {
                if (initializeData.get(j) < currentMinNum) {
                    currentMinNumIndex = j;
                    currentMinNum = initializeData.get(j);
                    currentMinNumIsChange = true;
                }
            }
            if (currentMinNumIsChange) {
                initializeData.set(currentMinNumIndex, initializeData.get(i));
                initializeData.set(i, currentMinNum);
                counterNum++;
            }
        }
        for (Integer initializeDatum : initializeData) {
            System.out.println(initializeDatum);
        }
        System.out.println("次数：" + counterNum);
    }


    public LinkedList<Integer> getInitializeData() {
        LinkedList<Integer> data = new LinkedList<Integer>();
        data.add(10);
        data.add(32);
        data.add(4);
        data.add(6);
        data.add(54);
        data.add(8);
        data.add(74);
        data.add(97);
        data.add(45);
        data.add(47);
        return data;
    }
}
