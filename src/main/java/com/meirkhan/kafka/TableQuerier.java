package com.meirkhan.kafka;

import com.mongodb.client.MongoCursor;
import org.bson.Document;

abstract class TableQuerier {
    protected MongoCursor<Document> cursor;

    public MongoCursor<Document> getCursor(){
        return cursor;
    }
}
