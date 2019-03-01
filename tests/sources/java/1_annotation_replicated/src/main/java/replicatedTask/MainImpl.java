package replicatedTask;

import types.Pair;


public class MainImpl {

    public static void initInitialP(Pair p) {
        p.setX(1);
        p.setY(1);
    }

    public static Pair globalTask(Pair p, int newX) {
        Pair res = new Pair(p);
        res.setX(newX);

        System.out.println("Pair X = " + res.getX());
        System.out.println("Pair Y = " + res.getY());
        return res;
    }

    public static void normalTask(Pair p) {
        System.out.println("Pair X = " + p.getX());
        System.out.println("Pair Y = " + p.getY());
    }

}
