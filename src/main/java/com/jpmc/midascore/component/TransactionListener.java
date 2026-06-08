package com.jpmc.midascore.component;

import com.jpmc.midascore.entity.TransactionRecord;
import com.jpmc.midascore.entity.UserRecord;
import com.jpmc.midascore.foundation.Incentive;
import com.jpmc.midascore.foundation.Transaction;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

@Component
public class TransactionListener {
    private final DatabaseConduit databaseConduit;
    private final IncentiveConduit incentiveConduit;

    public TransactionListener(DatabaseConduit databaseConduit, IncentiveConduit incentiveConduit) {
        this.databaseConduit = databaseConduit;
        this.incentiveConduit = incentiveConduit;
    }

    @Transactional
    @KafkaListener(topics = "${general.kafka-topic}", groupId = "midas-core")
    public void receive(Transaction transaction) {
        UserRecord sender = databaseConduit.findUserById(transaction.getSenderId());
        UserRecord recipient = databaseConduit.findUserById(transaction.getRecipientId());

        if (sender == null || recipient == null) {
            return;
        }

        if (sender.getBalance() < transaction.getAmount()) {
            return;
        }

        Incentive incentive = incentiveConduit.requestIncentive(transaction);
        float incentiveAmount = incentive.getAmount();

        sender.setBalance(sender.getBalance() - transaction.getAmount());
        recipient.setBalance(recipient.getBalance() + transaction.getAmount() + incentiveAmount);

        databaseConduit.save(sender);
        databaseConduit.save(recipient);

        TransactionRecord transactionRecord = new TransactionRecord(
                sender,
                recipient,
                transaction.getAmount(),
                incentiveAmount
        );

        databaseConduit.save(transactionRecord);

        if (sender.getId() == 9 || recipient.getId() == 9) {
            System.out.println("Wilbur balance is now: " + databaseConduit.findUserById(9).getBalance());
        }
    }
}