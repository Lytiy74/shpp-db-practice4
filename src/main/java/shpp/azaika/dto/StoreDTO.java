package shpp.azaika.dto;

import org.hibernate.validator.constraints.Length;

import java.util.UUID;

public class StoreDTO {
    private UUID id;
    @Length(min = 5, max = 255)
    private String address;

    public StoreDTO(UUID id, String address) {
        this.id = id;
        this.address = address;
    }

    public StoreDTO(String address) {
        this.address = address;
    }

    public UUID getId() {
        return id;
    }

    public String getAddress() {
        return address;
    }

    public void setId(UUID id) {
        this.id = id;
    }

    public void setAddress(String address) {
        this.address = address;
    }

    @Override
    public String toString() {
        return "StoreDTO{" + "id=" + id +
                ", address='" + address + '\'' +
                '}';
    }
}
