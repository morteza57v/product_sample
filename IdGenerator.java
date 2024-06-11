package net.navoshgaran.mavad;

public class IdGenerator {

    private long maxId;


    public IdGenerator(long maxId) {
        this.maxId = maxId;
    }

    public long getNextId() {

        if(maxId % 1000 == 0)
            System.out.println("maxId: " + maxId);

        return maxId++;
    }
}
