package com.trident.retail_analysis;

public class VolumeState {
    private float sales;
    private float cancellations;

    public VolumeState() {
        this.sales = 0.0f;
        this.cancellations = 0.0f;
    }

    public void addSale(float sale) {
        this.sales += sale;
    }

    public void addCancellation(float cancellation) {
        this.cancellations += cancellation;
    }

    public float getSales() {
        return sales;
    }

    public float getCancellations() {
        return cancellations;
    }
}
