import unittest
from unittest.mock import patch

from src.models import Transcription, TranscriptionStatus


class TranscriptionStatusTests(unittest.TestCase):
    def test_allowed_transitions(self):
        transitions = (
            (TranscriptionStatus.QUEUED, TranscriptionStatus.PROCESSING),
            (TranscriptionStatus.QUEUED, TranscriptionStatus.FAILED),
            (TranscriptionStatus.PROCESSING, TranscriptionStatus.SUCCEEDED),
            (TranscriptionStatus.PROCESSING, TranscriptionStatus.FAILED),
        )
        for current, target in transitions:
            with self.subTest(current=current, target=target):
                transcription = Transcription(status=current)
                with patch('src.models.transcription.time.time', return_value=1234):
                    transcription.transition_to(target)
                self.assertEqual(transcription.status, target)
                expected_finished_at = None if target == TranscriptionStatus.PROCESSING else 1234
                self.assertEqual(transcription.finished_at, expected_finished_at)

    def test_queued_task_cannot_succeed_before_processing(self):
        transcription = Transcription(status=TranscriptionStatus.QUEUED)
        with self.assertRaises(ValueError):
            transcription.transition_to(TranscriptionStatus.SUCCEEDED)
        self.assertEqual(transcription.status, TranscriptionStatus.QUEUED)
        self.assertIsNone(transcription.finished_at)

    def test_processing_task_cannot_return_to_queue(self):
        transcription = Transcription(status=TranscriptionStatus.PROCESSING)
        with self.assertRaises(ValueError):
            transcription.transition_to(TranscriptionStatus.QUEUED)
        self.assertEqual(transcription.status, TranscriptionStatus.PROCESSING)

    def test_terminal_states_cannot_change(self):
        for current in (TranscriptionStatus.SUCCEEDED, TranscriptionStatus.FAILED):
            for target in TranscriptionStatus:
                with self.subTest(current=current, target=target):
                    transcription = Transcription(status=current, finished_at=1234)
                    with self.assertRaises(ValueError):
                        transcription.transition_to(target)
                    self.assertEqual(transcription.status, current)
                    self.assertEqual(transcription.finished_at, 1234)
