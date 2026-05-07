<?php

namespace Cgonser\SwiftMailerDatabaseS3SpoolBundle\Transport;

use Cgonser\SwiftMailerDatabaseS3SpoolBundle\Entity\MailQueueTransport;
use Cgonser\SwiftMailerDatabaseS3SpoolBundle\Repository\MailQueueTransportRepository;
use Symfony\Component\Mailer\Transport;
use Symfony\Component\Mailer\Transport\TransportInterface;

class TransportChain
{
    /** @var MailQueueTransportRepository */
    private $transportRepository;

    /** @var array */
    private $transports = null;

    /** @var array<string, TransportInterface> */
    private $mailerTransports = [];

    public function __construct(MailQueueTransportRepository $transportRepository)
    {
        $this->transportRepository = $transportRepository;
        $this->loadMailerTransports();
    }

    public function addMailerTransport(TransportInterface $transport, string $alias): void
    {
        $this->mailerTransports[$alias] = $transport;
    }

    /**
     * @return array<string, TransportInterface>
     */
    public function getMailerTransports(): array
    {
        return $this->mailerTransports;
    }

    public function getMailerTransport(string $alias): TransportInterface
    {
        if (array_key_exists($alias, $this->mailerTransports)) {
            return $this->mailerTransports[$alias];
        }

        throw new \RuntimeException('No mailer transport was found.');
    }

    public function getTransport(string $alias): MailQueueTransport
    {
        foreach ($this->transports as $transport) {
            /** @var MailQueueTransport $transport */
            if ($transport->getAlias() == $alias) {
                return $transport;
            }
        }

        throw new \RuntimeException('No transport was found.');
    }

    /**
     * @param string[] $tags
     *
     * @return array{MailQueueTransport: ?MailQueueTransport, MailerTransport: TransportInterface}
     */
    public function getTransportByTags(array $tags): array
    {
        $score = [];
        $defaultTransport = null;

        foreach ($this->getTransports() as $transport) {
            /** @var MailQueueTransport $transport */
            $score[$transport->getAlias()] = 0;

            foreach ($transport->getTags() as $tag) {
                if (substr($tag, 0, 1) == '-' && in_array(substr($tag, 1), $tags, true)) {
                    $score[$transport->getAlias()] = 0;
                    break;
                }

                $score[$transport->getAlias()] += in_array($tag, $tags, true) ? 1 : 0;
            }

            if ($score[$transport->getAlias()] == 0) {
                unset($score[$transport->getAlias()]);
            }

            if ($transport->isDefault()) {
                $defaultTransport = $transport;
            }
        }

        if (count($score) > 0) {
            arsort($score);

            return [
                'MailQueueTransport' => $this->getTransport((string) key($score)),
                'MailerTransport' => $this->getMailerTransport((string) key($score)),
            ];
        }

        if ($defaultTransport instanceof MailQueueTransport) {
            return [
                'MailQueueTransport' => $defaultTransport,
                'MailerTransport' => $this->getMailerTransport($defaultTransport->getAlias()),
            ];
        }

        throw new \RuntimeException('No transports were found.');
    }

    /**
     * @return MailQueueTransport[]
     */
    protected function getTransports(): array
    {
        if ($this->transports == null) {
            $this->transports = $this->transportRepository->findBy(['enabled' => true]);
        }

        return $this->transports;
    }

    protected function loadMailerTransports(): void
    {
        foreach ($this->getTransports() as $transport) {
            /** @var MailQueueTransport $transport */
            $scheme = strtolower((string) $transport->getEncryption()) === 'ssl' ? 'smtps' : 'smtp';

            $dsn = sprintf(
                '%s://%s:%s@%s:%d',
                $scheme,
                rawurlencode((string) $transport->getUsername()),
                rawurlencode((string) $transport->getPassword()),
                (string) $transport->getHost(),
                (int) $transport->getPort()
            );

            $this->addMailerTransport(Transport::fromDsn($dsn), (string) $transport->getAlias());
        }
    }
}
