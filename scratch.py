def clean_ocr_text(text: str) -> str:
    if not text:
        return ""
    cleaned_words = []
    for word in text.split():
        alnum_count = sum(1 for c in word if c.isalnum())
        if len(word) > 0 and (alnum_count / len(word) >= 0.4):
            cleaned_words.append(word)
    return ' '.join(cleaned_words)

noisy = """O comers = aan, en °% sy "sa. ar ed > ein Theta semecames i Mobile Apps Contest Buddy h N a a a2 ^ . ; PNY - Nến tảng cuộc thi SN . hàng đầu Việt Nam Thun ha: roy Ỹ 2 ‘Kham pha, tham gia và kết nối với hàng nghĩn cuộc thị chất lượng. Xây dựng. portfolo cá nhền và tìm kiếm đối tác phù hợp với kỹ năng của bạn. aụ NGƯỜI ham Đa, Cordier) ys ED GED -¬-- ms : Khám phá cuöc mm. ON ON es : “ $ whe ye AN hi (ẩ. === Pare I x ry Y2' 1š ra" & © Quốc Ga (7 — cry (eens cy lì Ỳ ; — aa: Stem ; 7 - __—_— + Mobile Apps Contest Buddy ¬ a ° eX: " . b ` Dư ng TH ’ ca ` ON Pear) Giải thương t@... Lư) a onsen’ NA 1% i ‘a. grease ` \ onary es 7 ` CC "an “.... s. TL kem ¬ "— oar Feat Nee TC. Startupia ¬ on Bie Parents 7 ro l OO en asa) _ a"""

print("Original:")
print(noisy)
print("\nCleaned:")
print(clean_ocr_text(noisy))
