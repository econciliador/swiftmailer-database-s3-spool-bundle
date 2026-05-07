<?php

namespace Cgonser\SwiftMailerDatabaseS3SpoolBundle\Spool;

use Aws\S3\S3Client;
use Cgonser\SwiftMailerDatabaseS3SpoolBundle\Entity\MailQueue;
use Cgonser\SwiftMailerDatabaseS3SpoolBundle\Entity\MailQueueTransport;
use Cgonser\SwiftMailerDatabaseS3SpoolBundle\Transport\TransportChain;
use Doctrine\Bundle\DoctrineBundle\Registry;
use Doctrine\Common\Cache\CacheProvider;
use Doctrine\ORM\EntityManager;
use Enqueue\AmqpTools\RabbitMqDlxDelayStrategy;
use Interop\Amqp\AmqpContext;
use Interop\Amqp\AmqpQueue;
use Psr\Log\LoggerInterface;
use Symfony\Component\Mailer\Envelope;
use Symfony\Component\Mailer\Exception\TransportException;
use Symfony\Component\Mailer\SentMessage;
use Symfony\Component\Mailer\Transport\NullTransport;
use Symfony\Component\Mailer\Transport\TransportInterface;
use Symfony\Component\Mime\Address;
use Symfony\Component\Mime\Email;
use Symfony\Component\Mime\Header\MailboxListHeader;
use Symfony\Component\Mime\Message;
use Symfony\Component\Mime\RawMessage;

class DatabaseS3Spool implements TransportInterface
{
    /** @var S3Client */
    protected $s3Client;

    /** @var string */
    protected $s3Bucket;

    /** @var string */
    protected $s3Folder = '';

    /** @var string */
    protected $entityClass;

    /** @var Registry */
    protected $doctrine;

    /** @var EntityManager */
    protected $entityManager;

    /** @var AmqpContext */
    protected $amqpContext;

    /** @var AmqpQueue */
    protected $amqpQueue;

    /** @var TransportChain */
    protected $transportChain;

    /** @var CacheProvider|null */
    protected $cache;

    /** @var LoggerInterface */
    protected $logger;

    /** @var string[] */
    protected $failedRecipients = [];

    /** @var int */
    protected $messageLimit = 10;

    /** @var int|null */
    protected $timeLimit;

    /** @var int */
    private $maxRetries = 3;

    /** @var bool */
    private $disableDelivery = false;

    /** @var int|null */
    private $deduplicationPeriod;

    public function __construct($s3Config, $entityClass, Registry $doctrine, AmqpContext $amqpContext, TransportChain $transportChain, CacheProvider $cache = null, LoggerInterface $logger)
    {
        $this->s3Bucket = $s3Config['bucket'];
        unset($s3Config['bucket']);

        if (isset($s3Config['folder'])) {
            $this->s3Folder = $s3Config['folder'];
            unset($s3Config['folder']);
        }

        $this->s3Client = new S3Client($s3Config);

        $this->doctrine = $doctrine;
        $this->entityClass = $entityClass;
        $this->entityManager = $this->doctrine->getManagerForClass($this->entityClass);
        $this->amqpContext = $amqpContext;
        $this->transportChain = $transportChain;
        $this->cache = $cache;
        $this->logger = $logger;

        $this->setupQueue($amqpContext);
    }

    public function __toString(): string
    {
        return 'db+s3';
    }

    public function send(RawMessage $message, Envelope $envelope = null): ?SentMessage
    {
        $this->queueMessage($message, $envelope);

        return null;
    }

    public function flushQueue(&$failedRecipients = null): int
    {
        $this->failedRecipients = (array) $failedRecipients;

        return $this->sendMessages();
    }

    public function setMessageLimit(int $messageLimit): self
    {
        $this->messageLimit = $messageLimit;

        return $this;
    }

    public function getMessageLimit(): int
    {
        return $this->messageLimit;
    }

    public function setTimeLimit(?int $timeLimit): self
    {
        $this->timeLimit = $timeLimit;

        return $this;
    }

    public function getTimeLimit(): ?int
    {
        return $this->timeLimit;
    }

    protected function queueMessage(RawMessage $message, ?Envelope $envelope): bool
    {
        /** @var MailQueue $object */
        $object = new $this->entityClass();

        $from = '';
        if ($envelope instanceof Envelope && $envelope->getSender() instanceof Address) {
            $from = $envelope->getSender()->toString();
        } elseif ($message instanceof Message) {
            $fromCandidates = $this->extractAddressListFromHeader($message, 'From');
            $from = count($fromCandidates) > 0 ? $fromCandidates[0] : '';
        }

        $recipients = [];
        if ($envelope instanceof Envelope) {
            foreach ($envelope->getRecipients() as $recipient) {
                $recipients[] = $recipient->toString();
            }
        } elseif ($message instanceof Message) {
            $recipients = $this->extractAddressListFromHeader($message, 'To');
        }

        $subject = null;
        if ($message instanceof Email) {
            $subject = $message->getSubject();
            if ($subject !== null) {
                $subject = mb_substr($subject, 0, 255);
            }
        }

        $object->setSubject($subject);
        $object->setSender($from);
        $object->setRecipient(implode(';', $this->sanitizeAddresses($recipients)));

        if ($message instanceof Message) {
            $cc = $this->extractAddressListFromHeader($message, 'Cc');
            if (count($cc) > 0) {
                $object->setCc(implode(';', $this->sanitizeAddresses($cc)));
            }

            $bcc = $this->extractAddressListFromHeader($message, 'Bcc');
            if (count($bcc) > 0) {
                $object->setBcc(implode(';', $this->sanitizeAddresses($bcc)));
            }
        }

        $object->setQueuedAt(new \DateTime());
        $object->setDeduplicationHash($this->generateMessageDeduplicationHash($message));

        $this->entityManager->persist($object);
        $this->entityManager->flush();

        $result = $this->s3StoreMessage($object->getId(), $message);

        $this->queueMail($object);

        return $result;
    }

    protected function sendMessages(): int
    {
        $consumer = $this->amqpContext->createConsumer($this->amqpQueue);

        $limit = empty($this->getMessageLimit()) ? 10 : $this->getMessageLimit();

        $messages = [];
        for ($i = 0; $i <= $limit; $i++) {
            $message = $consumer->receive(3);
            if (empty($message)) {
                break;
            }
            $decodedMessageBody = json_decode($message->getBody(), true);
            $messages[$decodedMessageBody['id']] = $message;
        }

        if (!$messages || count($messages) === 0) {
            sleep(5);

            return 0;
        }

        $queuedMessages = $this->fetchMessages(array_keys($messages));

        $startTime = time();
        $count = 0;

        foreach ($queuedMessages as $mailQueueObject) {
            $mailQueueObject->setStartedAt(new \DateTime());
            $mailQueueObject->increaseRetriesCount();
            $this->entityManager->persist($mailQueueObject);
            $this->entityManager->flush();

            /** @var MailQueue $mailQueueObject */
            if (empty($mailQueueObject->getSentAt())) {
                $count += $this->sendMessage($mailQueueObject);
            }

            $consumer->acknowledge($messages[$mailQueueObject->getId()]);
            unset($messages[$mailQueueObject->getId()]);

            if ($this->getTimeLimit() && (time() - $startTime) >= $this->getTimeLimit()) {
                break;
            }
        }

        foreach ($messages as $pendingMessage) {
            $consumer->acknowledge($pendingMessage);
        }

        $this->entityManager->flush();

        return $count;
    }

    protected function sendMessage(MailQueue $mailQueueObject): int
    {
        try {
            $message = $this->s3RetrieveMessage($mailQueueObject->getId());

            $transport = [
                'MailerTransport' => new NullTransport(),
                'MailQueueTransport' => null,
            ];

            if ($this->isDisableDelivery() === false) {
                $tags = $this->getMessageTags($message);
                $transport = $this->transportChain->getTransportByTags($tags);

                $mailQueueObject->setMailQueueTransport($transport['MailQueueTransport']);
                $this->entityManager->persist($mailQueueObject);
            }

            if (
                $transport['MailQueueTransport'] instanceof MailQueueTransport
                && !empty($transport['MailQueueTransport']->getSender())
                && $message instanceof Email
            ) {
                $sender = $transport['MailQueueTransport']->getSender();
                if (preg_match('/(?P<sender_name>[\w\s\p{L}]+)<(?P<sender_address>[\w\.]+@[\w\.]+)>/ui', $sender, $matches)) {
                    $message->from(new Address(trim($matches['sender_address']), trim($matches['sender_name'])));
                } else {
                    $message->from($sender);
                }
                $mailQueueObject->setSender($sender);
                $this->entityManager->persist($mailQueueObject);
            }

            if ($transport['MailQueueTransport'] instanceof MailQueueTransport && $transport['MailQueueTransport']->isPaused()) {
                $mailQueueObject->setErrorMessage('Message delayed for one hour. The mail transport is paused.');
                $mailQueueObject->resetRetriesCount();
                $this->entityManager->persist($mailQueueObject);
                $this->queueMail($mailQueueObject, 60 * 60);

                return 0;
            }

            if (!empty($this->getDeduplicationPeriod())) {
                if (!$this->cache instanceof CacheProvider) {
                    throw new \RuntimeException('You must enable doctrine second level cache to use this feature.');
                }

                $mailQueueObject->setDeduplicationHash($this->generateMessageDeduplicationHash($message));
                $hashKey = '[cgonser_mail_queue][deduplication]['.$mailQueueObject->getDeduplicationHash().']';
                if ($id = $this->cache->fetch($hashKey)) {
                    $mailQueueObject->setErrorMessage('Sending cancelled. This message duplicates message id '.$id);
                    $this->entityManager->persist($mailQueueObject);

                    return 0;
                }
                $this->cache->save($hashKey, $mailQueueObject->getId(), $this->getDeduplicationPeriod());
            }

            $transport['MailerTransport']->send($message);

            $mailQueueObject->setSentAt(new \DateTime());
            $this->entityManager->persist($mailQueueObject);
            $this->entityManager->flush();
            $this->s3ArquiveMessage($mailQueueObject->getId());
        } catch (\Exception $e) {
            $this->logger->error((string) $e);

            if (!empty($this->getDeduplicationPeriod()) && !empty($mailQueueObject->getDeduplicationHash())) {
                $hashKey = '[cgonser_mail_queue][deduplication]['.$mailQueueObject->getDeduplicationHash().']';
                $this->cache->delete($hashKey);
            }
            $mailQueueObject->setErrorMessage($e->getMessage());
            if ($this->maxRetries >= $mailQueueObject->getRetries()) {
                $this->queueMail($mailQueueObject, 60 * 5 * ($mailQueueObject->getRetries() + 1));
            }
            $this->entityManager->persist($mailQueueObject);

            return 0;
        }

        return 1;
    }

    /**
     * @param int[] $messages
     *
     * @return MailQueue[]
     */
    protected function fetchMessages(array $messages = []): array
    {
        $qb = $this->entityManager->getRepository($this->entityClass)
            ->createQueryBuilder('m');
        $qb->andWhere($qb->expr()->in('m.id', ':ids'))
            ->setParameter(':ids', $messages);

        return $qb->getQuery()->getResult();
    }

    protected function s3StoreMessage(int $messageId, RawMessage $message): bool
    {
        $key = $messageId.'.msg';

        try {
            $this->s3Client->putObject([
                'Bucket' => $this->s3Bucket,
                'Key' => $this->buildS3Key($key),
                'Body' => serialize($message),
                'ACL' => 'private',
            ]);
        } catch (\Exception $e) {
            throw new TransportException(sprintf('Unable to store message "%s" in S3 Bucket "%s".', $messageId, $this->s3Bucket), 0, $e);
        }

        return true;
    }

    protected function s3RetrieveMessage(int $messageId): RawMessage
    {
        $key = $messageId.'.msg';

        try {
            $result = $this->s3Client->getObject([
                'Bucket' => $this->s3Bucket,
                'Key' => $this->buildS3Key($key),
            ]);

            $message = unserialize((string) $result['Body']);
            if (!$message instanceof RawMessage) {
                throw new \RuntimeException('Invalid message payload.');
            }

            return $message;
        } catch (\Exception $e) {
            throw new TransportException(sprintf('Unable to retrieve message "%s" from S3 Bucket "%s".', $messageId, $this->s3Bucket), 0, $e);
        }
    }

    protected function s3ArquiveMessage(int $messageId): void
    {
        $sourceKey = $messageId.'.msg';
        $targetKey = 'sent/'.date('Y/m/d').'/'.$messageId.'.msg';

        try {
            $copySource = $this->s3Bucket.'/'.ltrim($this->buildS3Key($sourceKey), '/');

            $this->s3Client->copyObject([
                'Bucket' => $this->s3Bucket,
                'Key' => $this->buildS3Key($targetKey),
                'CopySource' => $copySource,
            ]);

            $this->s3Client->deleteObject([
                'Bucket' => $this->s3Bucket,
                'Key' => $this->buildS3Key($sourceKey),
            ]);
        } catch (\Exception $e) {
            throw new TransportException(sprintf('Unable to archive message "%s" in S3 Bucket "%s".', $messageId, $this->s3Bucket), 0, $e);
        }
    }

    /**
     * @param string[] $addresses
     *
     * @return string[]
     */
    protected function sanitizeAddresses($addresses): array
    {
        return array_values(array_filter(array_map(
            static function ($email) {
                return filter_var(filter_var(trim((string) $email), FILTER_SANITIZE_EMAIL), FILTER_VALIDATE_EMAIL) ?: false;
            },
            (array) $addresses
        )));
    }

    protected function setupQueue(AmqpContext $context): void
    {
        $this->amqpQueue = $context->createQueue('cgonser_mail_queue');
        $this->amqpQueue->addFlag(AmqpQueue::FLAG_DURABLE);
        $context->declareQueue($this->amqpQueue);
    }

    protected function queueMail(MailQueue $object, int $delaySeconds = 0): void
    {
        $producer = $this->amqpContext->createProducer();
        if ($delaySeconds > 0) {
            $producer->setDelayStrategy(new RabbitMqDlxDelayStrategy())
                ->setDeliveryDelay($delaySeconds * 1000);
        }
        $producer->send(
            $this->amqpQueue,
            $this->amqpContext->createMessage(
                json_encode([
                    'id' => $object->getId(),
                ])
            )
        );
    }

    /**
     * @return string[]
     */
    protected function getMessageTags(RawMessage $message): array
    {
        if (!$message instanceof Message) {
            return [];
        }

        $tags = [];
        foreach ($message->getHeaders()->all('X-Mailer-Tag') as $tagHeader) {
            if (method_exists($tagHeader, 'getBodyAsString')) {
                $tags[] = $tagHeader->getBodyAsString();
            }
        }

        return $tags;
    }

    public function generateMessageDeduplicationHash(RawMessage $message): ?string
    {
        if ($message instanceof Email) {
            $string = implode(';', $this->extractAddressListFromHeader($message, 'To'));
            $string .= implode(';', $this->extractAddressListFromHeader($message, 'Cc'));
            $string .= implode(';', $this->extractAddressListFromHeader($message, 'Bcc'));
            $string .= (string) $message->getSubject();
            $string .= (string) $message->getTextBody();
            $string .= (string) $message->getHtmlBody();
        } else {
            $string = $message->toString();
        }

        if (empty($string)) {
            return null;
        }

        return hash('sha512', $string);
    }

    public function isDisableDelivery(): bool
    {
        return $this->disableDelivery;
    }

    public function setDisableDelivery($disableDelivery): DatabaseS3Spool
    {
        $this->disableDelivery = (bool) $disableDelivery;

        return $this;
    }

    public function getDeduplicationPeriod(): ?int
    {
        return $this->deduplicationPeriod;
    }

    public function setDeduplicationPeriod(?int $deduplicationPeriod): DatabaseS3Spool
    {
        $this->deduplicationPeriod = $deduplicationPeriod;

        return $this;
    }

    private function buildS3Key(string $key): string
    {
        if (empty($this->s3Folder)) {
            return ltrim($key, '/');
        }

        return trim($this->s3Folder, '/').'/'.ltrim($key, '/');
    }

    /**
     * @return string[]
     */
    private function extractAddressListFromHeader(Message $message, string $headerName): array
    {
        $header = $message->getHeaders()->get($headerName);
        if (!$header instanceof MailboxListHeader) {
            return [];
        }

        $addresses = [];
        foreach ($header->getAddresses() as $address) {
            if ($address instanceof Address) {
                $addresses[] = $address->toString();
            }
        }

        return $addresses;
    }
}
