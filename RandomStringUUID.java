package net.navoshgaran.mavad.cnv;

import java.util.UUID;

public class RandomStringUUID {

    public String getUUID() {
        UUID uuid = UUID.randomUUID();
        return uuid.toString();
    }

}
