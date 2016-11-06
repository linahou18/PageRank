
public class Driver {
    public static void main(String[] args) throws Exception {
        UnitMultiplication multiplication = new UnitMultiplication();
        UnitSum sum = new UnitSum();

        // args0 : directory of transition.txt
        // args1 : directory of pr.txt
        // args2 : directory of unitMultiplication result
        // args3 : times of iteration
        // args4 : beta val

        String transitionMatrix = args[0];
        String prMatrix = args[1];
        String subPR = args[2];
        int count = Integer.parseInt(args[3]);
        String beta = args[4];

        for (int i = 0; i < count; i++) {
            String[] args1 = {transitionMatrix, prMatrix + i, subPR + i, beta};
            multiplication.main(args1);
            String[] args2 = {subPR + i, prMatrix + i, prMatrix + (i + 1), beta};
            sum.main(args2);
        }
    }
}
