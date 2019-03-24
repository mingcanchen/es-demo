package com.ming.controller;

import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.format.annotation.DateTimeFormat;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.io.IOException;
import java.util.Date;

/**
 * @author wolter
 */
@RestController
public class ElasticSearchController {

    @Autowired
    private TransportClient client;

    @GetMapping("/")
    public String index() {
        return "index";
    }

    @GetMapping("/book/novel")
    public ResponseEntity get(@RequestParam(name = "id", defaultValue = "") String id) {
        if (id.isEmpty()) {
            return new ResponseEntity<>(HttpStatus.NOT_FOUND);
        }

        GetResponse result = client.prepareGet("book", "novel", id).get();

        if (!result.isExists()) {
            return new ResponseEntity<>(HttpStatus.NOT_FOUND);
        }

        return new ResponseEntity(result.getSource(), HttpStatus.OK);
    }

    @PostMapping("/book/novel")
    public ResponseEntity add(@RequestParam(name = "title") String title, @RequestParam(name = "author") String author,
                              @RequestParam(name = "word_count") int wordCount,
                              @RequestParam(name = "publish_date") @DateTimeFormat(pattern = "yyyy-MM-dd HH:mm:ss") Date publishDate) {
        try {
            XContentBuilder xContentBuilder = XContentFactory.jsonBuilder().startObject().field("title", title).field("author", author).field("word_count",
                    wordCount).field("publish_date", publishDate.getTime()).endObject();
            IndexResponse indexResponse = this.client.prepareIndex("book", "novel").setSource(xContentBuilder).get();

            return new ResponseEntity(indexResponse.getId(), HttpStatus.OK);

        } catch (IOException e) {
            return new ResponseEntity(HttpStatus.INTERNAL_SERVER_ERROR);
        }

    }

    @DeleteMapping("/book/novel")
    public ResponseEntity delete(@RequestParam(name = "id")String id) {
        DeleteResponse deleteResponse = this.client.prepareDelete("book", "novel", id).get();

        return new ResponseEntity(deleteResponse.getResult().toString(), HttpStatus.OK);
    }

    @PutMapping("/book/novel")
    public ResponseEntity update(@RequestParam(name = "id") String id,
                                 @RequestParam(name = "title",required = false) String title,
                                 @RequestParam(name = "author",required = false) String author) {
        UpdateRequest update = new UpdateRequest("book", "novel", id);
        try {
            XContentBuilder builder = XContentFactory.jsonBuilder().startObject();

            if (title != null) {
                builder.field("title", title);
            }

            if (author != null) {
                builder.field("author", author);
            }

            builder.endObject();
            update.doc(builder);

        } catch (IOException e) {
            return new ResponseEntity(HttpStatus.INTERNAL_SERVER_ERROR);
        }

        try {
            this.client.update(update).get();
        } catch (Exception e) {
            return new ResponseEntity(HttpStatus.INTERNAL_SERVER_ERROR);
        }

        return new ResponseEntity(HttpStatus.OK);
    }

}
