package es.bsc.compss.scheduler.multiobjective.types;

import es.bsc.compss.scheduler.types.Profile;
import es.bsc.compss.types.implementations.Implementation;
import es.bsc.compss.types.resources.Worker;
import es.bsc.compss.types.resources.WorkerResourceDescription;
import org.json.JSONException;
import org.json.JSONObject;


public class MOProfile extends Profile {

    private static final double DEFAULT_PRICE = 0;
    private static final double DEFAULT_POWER = 0;

    private double power;
    private double price;


    public MOProfile() {
        super();
        power = DEFAULT_POWER;
        price = DEFAULT_PRICE;
    }

    private MOProfile(MOProfile p) {
        super(p);
        this.power = p.power;
        this.price = p.price;
    }

    public MOProfile(JSONObject json) {
        super(json);
        if (json != null) {
            try {
                power = json.getDouble("power");
            } catch (JSONException je) {
                power = DEFAULT_POWER;
            }
            try {
                price = json.getDouble("price");
            } catch (JSONException je) {
                price = DEFAULT_PRICE;
            }
        } else {
            power = DEFAULT_POWER;
            price = DEFAULT_PRICE;
        }
    }

    public <T extends WorkerResourceDescription> MOProfile(Implementation impl, Worker<T> resource) {
        power = 0;
        price = 0;
    }

    public double getPower() {
        return power;
    }

    public double getPrice() {
        return price;
    }

    public void accumulate(MOProfile profile) {
        super.accumulate(profile);
    }

    @Override
    public JSONObject toJSONObject() {
        JSONObject jo = super.toJSONObject();
        jo.put("power", this.power);
        jo.put("price", this.price);
        return jo;
    }

    @Override
    public JSONObject updateJSON(JSONObject jo) {
        JSONObject difference = super.updateJSON(jo);

        double diff = this.power;
        if (jo.has("power")) {
            diff -= jo.getDouble("power");
        }
        difference.put("power", diff);
        jo.put("power", this.power);

        diff = this.price;
        if (jo.has("price")) {
            diff -= jo.getDouble("price");
        }
        difference.put("price", diff);
        jo.put("price", this.price);
        return difference;
    }

    @Override
    public Profile copy() {
        return new MOProfile(this);
    }

    @Override
    public String toString() {
        return "[MOProfile " + getContent() + "]";
    }

    @Override
    protected String getContent() {
        return super.getContent() + " power=" + power + " price=" + price;
    }


    public static class Builder extends Profile.Builder {

        private double power = DEFAULT_POWER;
        private double price = DEFAULT_PRICE;


        public Builder() {
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
            prof.power = this.power;
            prof.price = this.price;
        }
    }
}
