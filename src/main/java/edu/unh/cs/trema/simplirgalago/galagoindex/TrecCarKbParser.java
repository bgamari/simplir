//package edu.unh.cs.trema.simplirgalago.galagoindex;
//
//
//import co.nstant.in.cbor.model.Map;
//import co.nstant.in.cbor.model.Number;
//import org.lemurproject.galago.core.types.DocumentSplit;
//import org.lemurproject.galago.tupleflow.*;
//import org.lemurproject.galago.tupleflow.execution.Verified;
//import org.lemurproject.galago.utility.Parameters;
//import org.lemurproject.galago.utility.compression.VByte;
//import org.lemurproject.galago.core.parse.Tag;
//
//import java.io.BufferedInputStream;
//import java.io.BufferedReader;
//import java.io.IOException;
//import java.math.BigInteger;
//import java.util.*;
//import java.
//
//
//import co.nstant.in.cbor.CborDecoder;
//import co.nstant.in.cbor.CborException;
//import co.nstant.in.cbor.model.*;
//
//
//import org.lemurproject.galago.core.parse.*;
///**
// * User: dietz
// * Date: 1/14/17
// * Time: 6:23 PM
// */
//public class TrecCarKbParser extends DocumentStreamParser {
//
//    CborDecoder decode;
//    private final BufferedInputStream stream;
//    TagTokenizer tokenizer;
//
//    /** Creates a new instance of TrecTextParser */
//    public TrecCarKbParser(DocumentSplit split, Parameters p) throws IOException {
//        super(split, p);
//        stream = getBufferedInputStream(split);
//        decode = new CborDecoder(stream);
//        tokenizer = new TagTokenizer();
//    }
//
//    public Document nextDocument() throws IOException {
//        DataItem dataItem;
//        try {
//            dataItem = decode.decodeNext();
//        } catch (CborException e) {
//            throw new IOException("Error deserializing", e);
//        }
//
//        // End of file?
//        if (dataItem == null) {
//            return null;
//        }
//
//        List<DataItem> top = ((Array) dataItem).getDataItems();
//        switch (top.get(0).getTag().getValue()) {
//            case 0:
//                String docNo = ((UnicodeString) top.get(1)).getString();
//
//                StringBuilder sb = new StringBuilder();
//                Map fields = (Map) top.get(2);
//                int offset = 0;
//                java.util.Map<String, Integer> fieldStartTokenOffsets = new java.util.HashMap<>();
//                java.util.Map<String, Integer> fieldEndTokenOffsets = new java.util.HashMap<>();
//                for (DataItem k : fields.getKeys()) {
//                    String fieldName = ((UnicodeString) k).getString();
//                    fieldStartTokenOffsets.put(fieldName, offset);
//                    for (DataItem contents : ((Array) fields.get(k)).getDataItems()) {
//                        // contents :: (count, text)
//                        List<DataItem> fieldPair = ((Array) contents).getDataItems();
//                        BigInteger count = ((Number) fieldPair.get(0)).getValue();
//                        String text = ((UnicodeString) fieldPair.get(1)).getString();
//
//                        tokenizer.addField();
//
//                        for (int i = 0; i < count; i++) {
//                            sb += text;
//                        }
//                        offset += text.length() * count;
//                    }
//                }
//
//                Document document = new Document(docNo, fullText);
//                tokenizer.tokenize(document);
//
//                document.   metadata = new HashMap<>();
//
//                document.tags = new ArrayList<>();
//                document.tags.add(new org.lemurproject.galago.core.parse.Tag("field", Collections.EMPTY_MAP, 1, 20));
//                return document;
//
//            default:
//                throw new IOException("Unknown document type");
//        }
//    }
//
//    @Override
//    public void close() throws IOException {
//        stream.close();
//    }
//
//}