package com.cj.collections;

import java.util.Iterator;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

public class IterableBlockingQueue<T> implements  Iterable<T>{
    private Queue<T> queue = new ConcurrentLinkedQueue<T>();
    private boolean isDone=false;

    public void done(){
        isDone=true;
    }

    public void add(T object){
        queue.add(object);
    }

    public Integer size() {
        return queue.size();
    }

    @Override
    public Iterator<T> iterator() {
        return new Iterator<T>() {
            @Override
            public boolean hasNext() {
                try {
                    //TODO: Waiting 300ms is a naive solution to blocking.
                    while(queue.isEmpty() && !isDone) Thread.sleep(300);
                } catch (InterruptedException e) {}
                return !(queue.isEmpty() && isDone);
            }

            @Override
            public T next() {
                return queue.remove();
            }

            @Override
            public void remove() {
                queue.remove();
            }
        };
    }


}
