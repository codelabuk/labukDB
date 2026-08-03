package com.labuk.deltaflight.log.action;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * One line of a Delta commit JSON file. Each line is a single JSON object
 * with exactly one of these keys populated — Delta's log format doesn't use
 * a discriminator field, it relies on "whichever key is present is the
 * action type," so the wrapper mirrors that shape rather than trying to
 * force polymorphic deserialization onto it.
 *
 * Action kinds this engine doesn't need for reading (txn, cdc, domainMetadata)
 * are intentionally left unmodeled and silently ignored via
 * {@code ignoreUnknown = true} — this is a read engine, not a full protocol
 * implementation.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class SingleAction {

    @JsonProperty("add")
    private AddFile add;

    @JsonProperty("remove")
    private RemoveFile remove;

    @JsonProperty("metaData")
    private MetadataAction metaData;

    @JsonProperty("protocol")
    private ProtocolAction protocol;

    @JsonProperty("commitInfo")
    private CommitInfoAction commitInfo;

    public SingleAction() {
    }

    public static SingleAction ofAdd(AddFile add) {
        SingleAction a = new SingleAction();
        a.add = add;
        return a;
    }

    public static SingleAction ofMetaData(MetadataAction metaData) {
        SingleAction a = new SingleAction();
        a.metaData = metaData;
        return a;
    }

    public static SingleAction ofProtocol(ProtocolAction protocol) {
        SingleAction a = new SingleAction();
        a.protocol = protocol;
        return a;
    }

    public AddFile getAdd() {
        return add;
    }

    public RemoveFile getRemove() {
        return remove;
    }

    public MetadataAction getMetaData() {
        return metaData;
    }

    public ProtocolAction getProtocol() {
        return protocol;
    }

    public CommitInfoAction getCommitInfo() {
        return commitInfo;
    }
}
