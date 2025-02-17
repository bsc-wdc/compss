/*
 *  Copyright 2002-2025 Barcelona Supercomputing Center (www.bsc.es)
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 */
package es.bsc.compss.scheduler.fullgraph.multiobjective.types;

import es.bsc.compss.scheduler.types.Profile;
import es.bsc.compss.types.implementations.Implementation;
import es.bsc.compss.types.resources.Worker;
import es.bsc.compss.types.resources.WorkerResourceDescription;
import org.json.JSONException;
import org.json.JSONObject;


public class MOProfile extends Profile {

    protected static final double DEFAULT_PRICE = 0;
    protected static final double DEFAULT_POWER = 0;

    private double power;
    private double price;


    /**
     * Creates a new MOProfile instance.
     */
    public MOProfile() {
        super();
        this.power = DEFAULT_POWER;
        this.price = DEFAULT_PRICE;
    }

    /**
     * Creates a copy of the given profile {@code p}.
     * 
     * @param p Profile to copy.
     */
    private MOProfile(MOProfile p) {
        super(p);
        this.power = p.power;
        this.price = p.price;
    }

    /**
     * Creates a new profile instance from the given JSON information.
     * 
     * @param json JSONObject containing the profile information.
     */
    public MOProfile(JSONObject json) {
        super(json);
        if (json != null) {
            try {
                this.power = json.getDouble("power");
            } catch (JSONException je) {
                this.power = DEFAULT_POWER;
            }
            try {
                this.price = json.getDouble("price");
            } catch (JSONException je) {
                this.price = DEFAULT_PRICE;
            }
        } else {
            this.power = DEFAULT_POWER;
            this.price = DEFAULT_PRICE;
        }
    }

    /**
     * Creates a new profile instance for the given implementation and resource.
     * 
     * @param impl Associated implementation.
     * @param resource Associated resource.
     */
    public <T extends WorkerResourceDescription> MOProfile(Implementation impl, Worker<T> resource) {
        this.power = 0;
        this.price = 0;
    }

    /**
     * Returns the consumed power.
     * 
     * @return The consumer power.
     */
    public double getPower() {
        return this.power;
    }

    /**
     * Returns the consumed price.
     * 
     * @return The consumed price.
     */
    public double getPrice() {
        return this.price;
    }

    /**
     * Sets a new power value.
     * 
     * @param power New power value.
     */
    public void setPower(double power) {
        this.power = power;
    }

    /**
     * Sets a new price value.
     * 
     * @param price New price value.
     */
    public void setPrice(double price) {
        this.price = price;
    }

    /**
     * Accumulates the given profile into this one.
     * 
     * @param profile Profile to accumulate.
     */
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
        return super.getContent() + " power=" + this.power + " price=" + this.price;
    }

}
