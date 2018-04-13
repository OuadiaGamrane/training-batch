package com.octo.batch.reader;

import java.beans.XMLDecoder;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.List;

import com.octo.batch.Customer;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.support.IteratorItemReader;

public class CustomerItemReader implements ItemReader<Customer> {

    private final String filename;

    private ItemReader<Customer> delegate;

    public CustomerItemReader(final String filename) {
        this.filename = filename;
    }

    @Override
    public Customer read() throws Exception {
        if (delegate == null) {
            delegate = new IteratorItemReader<>(customers());
        }
        return delegate.read();
    }

    private List<Customer> customers() throws FileNotFoundException {
        try (XMLDecoder decoder = new XMLDecoder(new FileInputStream(filename))) {
            return (List<Customer>) decoder.readObject();
        } catch (ArrayIndexOutOfBoundsException e) {

        }

        return new ArrayList<>();
    }
}
