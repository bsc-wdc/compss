package es.bsc.compss.scheduler.multiobjective.types;

import es.bsc.compss.scheduler.types.ProfileBuilder;


public class MOProfileBuilder extends ProfileBuilder {

    private double power = MOProfile.DEFAULT_POWER;
    private double price = MOProfile.DEFAULT_PRICE;


    public MOProfileBuilder() {
        super();
    }

    public MOProfile build() {
        MOProfile p = new MOProfile();
        update(p);
        return p;
    }

    public void setPower(double power) {
        this.power = power;
    }

    public void setPrice(double price) {
        this.price = price;
    }

    protected <P extends MOProfile> void update(P p) {
        super.update(p);
        MOProfile prof = (MOProfile) p;
        prof.setPower(this.power);
        prof.setPrice(this.price);
    }
}
