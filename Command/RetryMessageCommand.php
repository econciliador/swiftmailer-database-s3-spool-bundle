<?php
/**
 * Created by PhpStorm.
 * User: Rafael
 * Date: 10/31/2016
 * Time: 5:54 PM
 */

namespace Cgonser\SwiftMailerDatabaseS3SpoolBundle\Command;


use Cgonser\SwiftMailerDatabaseS3SpoolBundle\Entity\MailQueue;
use Interop\Amqp\AmqpQueue;
use Symfony\Bundle\FrameworkBundle\Command\ContainerAwareCommand;
use Symfony\Component\Console\Input\InputInterface;
use Symfony\Component\Console\Input\InputOption;
use Symfony\Component\Console\Output\OutputInterface;

class RetryMessageCommand extends ContainerAwareCommand
{

    /**
     * {@inheritdoc}
     */
    protected function configure()
    {
        $this
            ->setName('cgonser:mailer:retry')
            ->setDescription('Retry failed messages')
            ->addOption('message_limit', null, InputOption::VALUE_OPTIONAL, 'Messages Limit', 1000);
    }

    /**
     * {@inheritdoc}
     */
    protected function execute(InputInterface $input, OutputInterface $output)
    {
        $start = microtime(true);
        $container = $this->getContainer();
        $logger = $container->get('logger');
        $logger->info('Executing command ' . $this->getName());

        $em = $container->get('doctrine');

        $messages = $em->getRepository(MailQueue::class)->getFailedMessages($input->getOption('message_limit'));

        $logger->info("Retrying ".count($messages)." messages...");

        $context = $container->get('enqueue.transport.default.context');

        $amqpQueue = $context->createQueue('cgonser_mail_queue');
        $amqpQueue->addFlag(AmqpQueue::FLAG_DURABLE);
        $context->declareQueue($amqpQueue);

        $producer = $context->createProducer();

        foreach ($messages as $message) {
            /** @var MailQueue $message */
            $producer->send(
                $amqpQueue,
                $context->createMessage(
                    json_encode([
                        'id' => $message->getId()
                    ])
                )
            );
        }


    }
}