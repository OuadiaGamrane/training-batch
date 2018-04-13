package com.octo.batch.processor;

import java.util.Calendar;
import java.util.GregorianCalendar;

import com.octo.batch.model.Customer;
import org.springframework.batch.item.ItemProcessor;

public class BirthdayFilterProcessor implements ItemProcessor<Customer, Customer> {
    @Override
    public Customer process(final Customer item) throws Exception {
        if (new GregorianCalendar().get(Calendar.MONTH) == item.getBirthday().get(Calendar.MONTH)) {
            return item;
        }
        return null;
    }
}
