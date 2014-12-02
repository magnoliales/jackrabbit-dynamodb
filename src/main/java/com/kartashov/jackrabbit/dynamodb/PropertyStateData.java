package com.kartashov.jackrabbit.dynamodb;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.jackrabbit.core.id.PropertyId;
import org.apache.jackrabbit.core.persistence.PersistenceManager;
import org.apache.jackrabbit.core.state.PropertyState;
import org.apache.jackrabbit.core.value.InternalValue;
import org.apache.jackrabbit.spi.Name;
import org.apache.jackrabbit.spi.commons.name.NameFactoryImpl;

import javax.jcr.PropertyType;
import javax.jcr.RepositoryException;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;

public class PropertyStateData {

    private String type;
    private boolean multiValued;
    private short modCount;
    private List<Object> values;

    @JsonCreator
    public PropertyStateData(@JsonProperty("type") String type,
                             @JsonProperty("multiValued") boolean multiValued,
                             @JsonProperty("modCount") short modCount,
                             @JsonProperty("values") List<Object> values) {
        this.type = type;
        this.multiValued = multiValued;
        this.modCount = modCount;
        this.values = values;
    }

    public PropertyStateData(PropertyState propertyState) throws RepositoryException {
        this.type = PropertyType.nameFromValue(propertyState.getType());
        this.multiValued = propertyState.isMultiValued();
        this.modCount = propertyState.getModCount();
        values = new ArrayList<>();
        for (InternalValue internalValue : propertyState.getValues()) {
            switch (propertyState.getType()) {
                case PropertyType.BOOLEAN:
                    values.add(internalValue.getBoolean());
                    break;
                case PropertyType.DATE:
                    // @todo add better format than this one
                    values.add(internalValue.getDate().getTimeInMillis());
                    break;
                case PropertyType.DOUBLE:
                    values.add(internalValue.getDouble());
                    break;
                case PropertyType.LONG:
                    values.add(internalValue.getLong());
                    break;
                case PropertyType.NAME:
                    values.add(internalValue.getName().toString());
                    break;
                case PropertyType.STRING:
                    values.add(internalValue.getString());
                    break;
                default:
                    throw new AssertionError("Not implemented for " + type);
            }
        }
    }

    public PropertyState toPropertyState(PersistenceManager pm, PropertyId propertyId) {
        PropertyState propertyState = pm.createNew(propertyId);
        propertyState.setType(PropertyType.valueFromName(type));
        propertyState.setMultiValued(multiValued);
        propertyState.setModCount(modCount);
        List<InternalValue> internalValues = new ArrayList<>();
        for (Object value : values) {
            switch (propertyState.getType()) {
                case PropertyType.BOOLEAN:
                    internalValues.add(InternalValue.create((Boolean) value));
                    break;
                case PropertyType.DATE:
                    Calendar calendar = Calendar.getInstance();
                    calendar.setTimeInMillis((Long) value);
                    internalValues.add(InternalValue.create(calendar));
                    break;
                case PropertyType.DOUBLE:
                    internalValues.add(InternalValue.create((Double) value));
                    break;
                case PropertyType.NAME:
                    Name name = NameFactoryImpl.getInstance().create((String) value);
                    internalValues.add(InternalValue.create(name));
                case PropertyType.STRING:
                    internalValues.add(InternalValue.create((String) value));
                    break;
                default:
                    throw new AssertionError("Not implemented for " + type);
            }
        }
        propertyState.setValues(internalValues.toArray(new InternalValue[internalValues.size()]));
        return propertyState;
    }

    @JsonProperty
    public String getType() {
        return type;
    }

    @JsonProperty
    public boolean isMultiValued() {
        return multiValued;
    }

    @JsonProperty
    public short getModCount() {
        return modCount;
    }

    @JsonProperty
    public List<Object> getValues() {
        return values;
    }
}
